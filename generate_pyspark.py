"""
generate_pyspark.py
Generates PySpark code from PowerCenter XML.
Handles multiple databases, mapplets, and complex expressions.
"""

import sys
from collections import deque
from powercenter_parser import PowerCenterParser, Folder, Mapping, Transformation, Port
from expression_compiler import translate_expression

# ----------------------------------------------------------------------
# JDBC helper – with multi‑line SQL support
# ----------------------------------------------------------------------
def jdbc_read(table_or_query: str, conn_name: str, is_query: bool = False) -> str:
    """
    Generate a Spark JDBC read string.
    Converts multi‑line SQL to a single line to avoid syntax errors.
    """
    if is_query:
        # Collapse newlines and multiple spaces into a single space
        query = ' '.join(table_or_query.split())
        # Escape triple quotes inside the query
        query = query.replace('"""', '\\"\\"\\"')
        dbtable = f'"""({query}) as subq"""'
    else:
        dbtable = f'"{table_or_query}"'
    return f"""spark.read.format("jdbc") \\
    .option("url", CONNECTIONS["{conn_name}"]["url"]) \\
    .option("user", CONNECTIONS["{conn_name}"]["user"]) \\
    .option("password", CONNECTIONS["{conn_name}"]["password"]) \\
    .option("driver", CONNECTIONS["{conn_name}"]["driver"]) \\
    .option("dbtable", {dbtable}) \\
    .load()"""

# ----------------------------------------------------------------------
# Graph helpers
# ----------------------------------------------------------------------
def build_upstream_map(transformations: dict, connectors: list) -> dict:
    upstream = {name: [] for name in transformations}
    for conn in connectors:
        if conn.to_instance in upstream:
            upstream[conn.to_instance].append(conn.from_instance)
    return upstream

def topological_sort(transformations: dict, upstream: dict) -> list:
    adj = {name: [] for name in transformations}
    in_degree = {name: 0 for name in transformations}
    for name, ups in upstream.items():
        for u in ups:
            if u in adj:
                adj[u].append(name)
                in_degree[name] += 1
    queue = deque([n for n, deg in in_degree.items() if deg == 0])
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in adj[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    if len(order) != len(transformations):
        order = list(transformations.keys())
    return order

# ----------------------------------------------------------------------
# Transformation handlers
# ----------------------------------------------------------------------
def gen_source_qualifier(trans: Transformation, folder: Folder, upstream: list, prefix=""):
    sql = trans.attributes.get("Sql Query", "").strip()
    if sql:
        sql = ' '.join(sql.split())       # collapse to single line
        db_name = trans.database_name or "OLTP"
        return f'df_{prefix}{trans.name} = {jdbc_read(sql, db_name, is_query=True)}\n'
    else:
        return f'# No SQL override for Source Qualifier {trans.name}\n'

def gen_expression(trans: Transformation, upstream: list, mapping, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    variables = mapping.variables if mapping else {}
    selections = []
    for port in trans.ports:
        if port.porttype in ("OUTPUT", "INPUT/OUTPUT"):
            expr_str = (port.expression or "").strip()
            if not expr_str or expr_str == port.name:
                col_expr = f'F.col("{port.name}")'
            else:
                try:
                    col_expr = translate_expression(expr_str, variables)
                except Exception as e:
                    col_expr = f'F.expr("""{expr_str}""")  # ERROR translating: {e}'
            selections.append(f'    {col_expr}.alias("{port.name}")')
    if selections:
        return f'df_{prefix}{trans.name} = df_{prefix}{upstream_name}.select(\n' + ',\n'.join(selections) + '\n)\n'
    else:
        return f'df_{prefix}{trans.name} = df_{prefix}{upstream_name}\n'

def gen_filter(trans: Transformation, upstream: list, mapping, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    condition = trans.attributes.get("Filter Condition", "").strip()
    if not condition:
        condition = "1 = 1"
    try:
        col_cond = translate_expression(condition, mapping.variables if mapping else {})
    except Exception as e:
        col_cond = f'F.expr("""{condition}""")  # ERROR: {e}'
    return f'df_{prefix}{trans.name} = df_{prefix}{upstream_name}.filter({col_cond})\n'

def gen_lookup(trans: Transformation, upstream: list, folder: Folder, mapping, prefix=""):
    # Get reusable definition if needed
    real_trans = None
    if trans.origin in ("mapping_instance", "mapplet_instance"):
        real_name = trans.attributes.get("TRANSFORMATION_NAME", "")
        if real_name:
            real_trans = folder.reusable_transformations.get(real_name)

    def get_attr(name, default=""):
        val = trans.attributes.get(name)
        if val is not None and val != "":
            return val
        if real_trans:
            return real_trans.attributes.get(name, default)
        return default

    sql_override = get_attr("Lookup Sql Override", "").strip()
    conn_info = trans.connection_info or get_attr("Connection Information", "")
    if conn_info.startswith("$"):
        conn_name = conn_info
    else:
        conn_name = conn_info if conn_info else "OLAP"

    if sql_override:
        sql_override = ' '.join(sql_override.split())   # collapse
        lookup_df = jdbc_read(sql_override, conn_name, is_query=True)
    else:
        table_name = get_attr("Lookup table name", trans.name)
        lookup_df = jdbc_read(table_name, conn_name, is_query=False)

    condition = get_attr("Lookup condition", "").strip()
    if not condition:
        print(f"WARNING: Lookup {trans.name} has no join condition. Using cross join.")
        condition = "F.lit(1) == F.lit(1)"
    else:
        try:
            condition = translate_expression(condition, mapping.variables if mapping else {})
        except Exception as e:
            print(f"WARNING: Failed to translate lookup condition for {trans.name}: {e}")
            condition = "F.lit(1) == F.lit(1)"

    upstream_name = upstream[0] if upstream else None
    if upstream_name is None:
        upstream_placeholder = f"df_{prefix}INPUT" if prefix else "???"
        print(f"WARNING: Lookup {trans.name} has no upstream defined. Using {upstream_placeholder}")
        upstream_name = upstream_placeholder

    return f"""df_lkp_{prefix}{trans.name} = {lookup_df}
df_{prefix}{trans.name} = df_{prefix}{upstream_name}.join(df_lkp_{prefix}{trans.name}, on={condition}, how='left')
"""

def gen_joiner(trans: Transformation, upstream: list, mapping, prefix=""):
    if len(upstream) < 2:
        return f"# Joiner {trans.name} requires at least 2 upstreams\n"
    left = upstream[0]
    right = upstream[1]
    join_type = trans.attributes.get("Join Type", "Normal Join")
    spark_join = "inner"
    if "Full Outer" in join_type:
        spark_join = "full"
    elif "Left Outer" in join_type:
        spark_join = "left"
    elif "Right Outer" in join_type:
        spark_join = "right"
    condition = trans.attributes.get("Join Condition", "1 = 1")
    try:
        col_cond = translate_expression(condition, mapping.variables if mapping else {})
    except Exception as e:
        col_cond = f'F.expr("""{condition}""") # ERROR: {e}'
    return f"""df_{prefix}{trans.name} = df_{prefix}{left}.join(df_{prefix}{right}, on={col_cond}, how='{spark_join}')
"""

def gen_aggregator(trans: Transformation, upstream: list, mapping, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    group_cols = []
    agg_exprs = []
    for port in trans.ports:
        if port.porttype in ("INPUT", "INPUT/OUTPUT") and not port.expression:
            group_cols.append(port.name)
        elif port.expression:
            expr_str = port.expression.strip()
            if expr_str:
                try:
                    col_expr = translate_expression(expr_str, mapping.variables if mapping else {})
                except Exception as e:
                    col_expr = f'F.expr("""{expr_str}""") # ERROR: {e}'
                agg_exprs.append(f'{col_expr}.alias("{port.name}")')
            else:
                agg_exprs.append(f'F.col("{port.name}")')
        else:
            group_cols.append(port.name)
    if group_cols:
        group_str = ", ".join([f'F.col("{c}")' for c in group_cols])
        agg_str = ", ".join(agg_exprs)
        return f"df_{prefix}{trans.name} = df_{prefix}{upstream_name}.groupBy({group_str}).agg({agg_str})\n"
    else:
        return f"df_{prefix}{trans.name} = df_{prefix}{upstream_name}\n"

def gen_router(trans: Transformation, upstream: list, mapping, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    lines = [f"# Router transformation: {trans.name} (not yet fully implemented)"]
    lines.append(f"df_{prefix}{trans.name} = df_{prefix}{upstream_name}  # Router - pass-through")
    return "\n".join(lines) + "\n"

def gen_sequence(trans: Transformation, upstream: list, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    start = trans.attributes.get("Start Value", "1")
    return f"""df_{prefix}{trans.name} = df_{prefix}{upstream_name}.withColumn(
    "SEQUENCE_ID", (F.monotonically_increasing_id() + F.lit({start})).cast("long")
)\n"""

def gen_target(trans: Transformation, upstream: list, prefix=""):
    upstream_name = upstream[0] if upstream else "???"
    target_table = trans.attributes.get("TRANSFORMATION_NAME", trans.name)
    db_name = trans.database_name or "OLAP"
    cols = []
    for port in trans.ports:
        if port.source_transform and port.source_port:
            cols.append(f'F.col("{port.source_port}").alias("{port.name}")')
        else:
            cols.append(f'F.lit(None).alias("{port.name}")')
    cols_str = ",\n    ".join(cols)
    return f"""df_target_{trans.name} = df_{prefix}{upstream_name}.select(
    {cols_str}
)
df_target_{trans.name}.write.format("jdbc") \\
    .option("url", CONNECTIONS["{db_name}"]["url"]) \\
    .option("user", CONNECTIONS["{db_name}"]["user"]) \\
    .option("password", CONNECTIONS["{db_name}"]["password"]) \\
    .option("driver", CONNECTIONS["{db_name}"]["driver"]) \\
    .option("dbtable", "{target_table}") \\
    .mode("overwrite") \\
    .save()
"""

def generate_trans_code(trans, ttype, upstream_list, folder, mapping, prefix=""):
    if ttype == "Source Qualifier":
        return gen_source_qualifier(trans, folder, upstream_list, prefix)
    elif ttype == "Expression":
        return gen_expression(trans, upstream_list, mapping, prefix)
    elif ttype == "Filter":
        return gen_filter(trans, upstream_list, mapping, prefix)
    elif ttype == "Lookup Procedure":
        return gen_lookup(trans, upstream_list, folder, mapping, prefix)
    elif ttype == "Joiner":
        return gen_joiner(trans, upstream_list, mapping, prefix)
    elif ttype == "Aggregator":
        return gen_aggregator(trans, upstream_list, mapping, prefix)
    elif ttype == "Router":
        return gen_router(trans, upstream_list, mapping, prefix)
    elif ttype == "Sequence":
        return gen_sequence(trans, upstream_list, prefix)
    elif ttype == "Target Definition":
        return gen_target(trans, upstream_list, prefix)
    else:
        return f"# Transformation {trans.name} of type {ttype} - not yet implemented\n"

# ----------------------------------------------------------------------
# Mapplet expansion (identical to previous version, uses same handlers)
# ----------------------------------------------------------------------
def generate_mapplet_code(instance_name: str, mapplet_def, folder, upstream_df_name: str, mapping_variables: dict, parser) -> (str, str):
    parser.resolve_connections_in_mapplet(mapplet_def, folder)

    upstream = build_upstream_map(mapplet_def.transformations, mapplet_def.connectors)
    order = topological_sort(mapplet_def.transformations, upstream)

    prefix = f"mplt_{instance_name}_"
    lines = []

    input_trans_name = None
    output_trans_name = None
    for tname, trans in mapplet_def.transformations.items():
        if trans.type == "Input Transformation":
            input_trans_name = tname
        elif trans.type == "Output Transformation":
            output_trans_name = tname

    if input_trans_name is None:
        raise ValueError(f"Mapplet {mapplet_def.name} has no Input Transformation")
    if output_trans_name is None:
        raise ValueError(f"Mapplet {mapplet_def.name} has no Output Transformation")

    class DummyMapping:
        def __init__(self, variables):
            self.variables = variables
    dummy_mapping = DummyMapping(mapplet_def.variables)

    lines.append(f"df_{prefix}{input_trans_name} = {upstream_df_name}")

    for tname in order:
        if tname in (input_trans_name, output_trans_name):
            continue
        trans = mapplet_def.transformations[tname]
        ups = upstream[tname]
        if not ups:
            ups = [input_trans_name]
            print(f"Warning: Transformation {tname} has no upstream; using {input_trans_name} as fallback.")
        ttype = trans.type
        code = generate_trans_code(trans, ttype, ups, folder, dummy_mapping, prefix=prefix)
        lines.append(code)

    out_ups = upstream.get(output_trans_name, [])
    if out_ups:
        output_df_name = f"df_{prefix}{out_ups[0]}"
    else:
        output_df_name = f"df_{prefix}{order[-1]}" if order else None

    if output_df_name is None:
        raise ValueError(f"Cannot determine output DataFrame for mapplet {instance_name}")

    output_trans = mapplet_def.transformations[output_trans_name]
    if output_trans.ports:
        selections = []
        for port in output_trans.ports:
            if port.source_transform and port.source_port:
                selections.append(f'F.col("{port.source_port}").alias("{port.name}")')
            else:
                selections.append(f'F.col("{port.name}")')
        if selections:
            select_str = ",\n    ".join(selections)
            lines.append(f"df_{prefix}{output_trans_name} = {output_df_name}.select(\n    {select_str}\n)")
            output_df_name = f"df_{prefix}{output_trans_name}"

    return "\n".join(lines), output_df_name

# ----------------------------------------------------------------------
# Main generation routine
# ----------------------------------------------------------------------
def generate_script(parser: PowerCenterParser, folder_name: str, mapping_name: str, output_path: str):
    folder = parser.repo.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder {folder_name} not found")
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        raise ValueError(f"Mapping {mapping_name} not found")

    parser.resolve_connections_in_mapping(mapping, folder)

    upstream = build_upstream_map(mapping.transformations, mapping.connectors)
    order = topological_sort(mapping.transformations, upstream)

    with open(output_path, 'w') as f:
        f.write(f"# Auto-generated PySpark script for mapping: {mapping_name}\n\n")
        f.write("from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\n\n")
        f.write("# Import connection dictionary\n")
        f.write("try:\n")
        f.write("    from db_config import CONNECTIONS\n")
        f.write("except ImportError:\n")
        f.write("    raise ImportError(\"Please create db_config.py with a CONNECTIONS dictionary.\")\n\n")
        f.write("try:\n    spark\nexcept NameError:\n    spark = SparkSession.builder.appName('{0}').getOrCreate()\n\n".format(mapping_name))

        for name in order:
            trans = mapping.transformations[name]
            ups = upstream[name]
            ttype = trans.type

            if ttype == "Source Definition":
                continue
            if ttype == "Target Definition" and not ups:
                continue

            if ttype == "Mapplet":
                mapplet_name = trans.attributes.get("TRANSFORMATION_NAME", "")
                if not mapplet_name:
                    code = f"# Mapplet {name} has no definition name\n"
                else:
                    mapplet_def = folder.mapplets.get(mapplet_name)
                    if not mapplet_def:
                        code = f"# Mapplet definition {mapplet_name} not found\n"
                    else:
                        upstream_name = ups[0] if ups else None
                        upstream_df = f"df_{upstream_name}" if upstream_name else "spark.range(1)"
                        mpl_code, mpl_output = generate_mapplet_code(
                            name, mapplet_def, folder, upstream_df, mapping.variables, parser
                        )
                        code = f"{mpl_code}\ndf_{name} = {mpl_output}\n"
                f.write(code)
                continue

            code = generate_trans_code(trans, ttype, ups, folder, mapping, prefix="")
            f.write(code)

        f.write("# End of script\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_pyspark.py <xml_file>")
        sys.exit(1)

    xml_file = sys.argv[1]
    parser = PowerCenterParser()
    repo = parser.parse(xml_file)

    if not repo.folders:
        raise ValueError("No folders found in repository")
    folder_name = list(repo.folders.keys())[0]
    folder = repo.folders[folder_name]
    if not folder.mappings:
        raise ValueError(f"No mappings found in folder '{folder_name}'")
    mapping_name = list(folder.mappings.keys())[0]

    output_script = f"{mapping_name}.py"
    output_script = output_script.replace(' ', '_').replace('/', '_').replace('\\', '_')

    print(f"Processing: folder='{folder_name}', mapping='{mapping_name}'")
    generate_script(parser, folder_name, mapping_name, output_script)
    print(f"Generated {output_script}")