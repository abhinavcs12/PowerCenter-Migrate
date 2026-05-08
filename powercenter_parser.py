"""
powercenter_parser.py
Parser for Informatica PowerCenter XML, based on real XML structure from paths.txt.
Now copies attributes from reusable transformations to instances.
"""

import json
from lxml import etree
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import re
import sys


# ----------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------
@dataclass
class Port:
    name: str
    data_type: str = ""
    precision: str = ""
    scale: str = ""
    porttype: str = ""          # INPUT, OUTPUT, INPUT/OUTPUT
    default_value: str = ""
    description: str = ""
    expression: str = ""        # original expression text
    parent_transform_name: str = ""   # name of the transformation containing this port
    # Resolved connection (filled later)
    source_transform: Optional[str] = None
    source_port: Optional[str] = None

@dataclass
class Transformation:
    name: str
    type: str                 # Source Definition, Target Definition, Expression, Source Qualifier, etc.
    reusable: bool = False
    ports: List[Port] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)   # TABLEATTRIBUTE and other properties
    origin: str = ""          # 'source', 'target', 'mapping', 'mapplet', 'reusable'

@dataclass
class Connector:
    from_instance: str
    from_field: str
    from_instance_type: str
    to_instance: str
    to_field: str
    to_instance_type: str

@dataclass
class Mapping:
    name: str
    transformations: Dict[str, Transformation] = field(default_factory=dict)   # keyed by instance name or transformation name
    connectors: List[Connector] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)   # name -> default value

@dataclass
class Mapplet:
    name: str
    transformations: Dict[str, Transformation] = field(default_factory=dict)
    connectors: List[Connector] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)

@dataclass
class Folder:
    name: str
    sources: Dict[str, Transformation] = field(default_factory=dict)   # name -> Transformation
    targets: Dict[str, Transformation] = field(default_factory=dict)
    reusable_transformations: Dict[str, Transformation] = field(default_factory=dict)
    mapplets: Dict[str, Mapplet] = field(default_factory=dict)
    mappings: Dict[str, Mapping] = field(default_factory=dict)

@dataclass
class Repository:
    name: str
    folders: Dict[str, Folder] = field(default_factory=dict)


# ----------------------------------------------------------------------
# Expression tokenizer (simplified, kept for compatibility)
# ----------------------------------------------------------------------
def tokenize(expr: str) -> List[str]:
    expr = re.sub(r'([(),+*/=<>!]+)', r' \1 ', expr)
    expr = re.sub(r"('(?:[^'\\]|\\.)*')", r' \1 ', expr)
    return [t for t in expr.split() if t]

def parse_expression_tokens(tokens: List[str]) -> Any:
    if not tokens:
        return None
    token = tokens.pop(0)
    if token.isupper() and tokens and tokens[0] == '(':
        func_name = token
        tokens.pop(0)  # '('
        args = []
        while tokens and tokens[0] != ')':
            args.append(parse_expression_tokens(tokens))
            if tokens and tokens[0] == ',':
                tokens.pop(0)
        if tokens and tokens[0] == ')':
            tokens.pop(0)
        return {"type": "function", "name": func_name, "arguments": args}
    elif token.replace('.', '', 1).isdigit():
        return {"type": "literal", "value": token}
    elif token.startswith("'") and token.endswith("'"):
        return {"type": "literal", "value": token[1:-1]}
    else:
        return {"type": "column_ref", "name": token}

def parse_expression(expr: str) -> Optional[Dict]:
    if not expr:
        return None
    return parse_expression_tokens(tokenize(expr))


# ----------------------------------------------------------------------
# Main parser
# ----------------------------------------------------------------------
class PowerCenterParser:
    def __init__(self):
        self.repo = Repository(name="")

    def parse(self, xml_path: str) -> Repository:
        tree = etree.parse(xml_path)
        root = tree.getroot()       # POWERMART
        repo_elem = root.find("REPOSITORY")
        if repo_elem is None:
            raise ValueError("Missing REPOSITORY element")
        self.repo.name = repo_elem.get("NAME", "Unknown")

        for folder_elem in repo_elem.findall("FOLDER"):
            folder_name = folder_elem.get("NAME", "Unknown")
            folder = Folder(name=folder_name)
            self._parse_folder_contents(folder_elem, folder)
            self.repo.folders[folder_name] = folder

        # After parsing all folders, we could resolve cross‑folder references,
        # but typically mappings and mapplets are self‑contained.
        return self.repo

    def _parse_folder_contents(self, folder_elem, folder: Folder):
        # Parse sources
        for src_elem in folder_elem.findall("SOURCE"):
            t = self._parse_source_or_target(src_elem, origin="source")
            folder.sources[t.name] = t
        # Parse targets
        for tgt_elem in folder_elem.findall("TARGET"):
            t = self._parse_source_or_target(tgt_elem, origin="target")
            folder.targets[t.name] = t
        # Parse folder‑level reusable transformations
        for trans_elem in folder_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="reusable")
            folder.reusable_transformations[t.name] = t
        # Parse mapplets
        for mpl_elem in folder_elem.findall("MAPPLET"):
            mpl = self._parse_mapplet(mpl_elem)
            folder.mapplets[mpl.name] = mpl
        # Parse mappings
        for map_elem in folder_elem.findall("MAPPING"):
            mapping = self._parse_mapping(map_elem)
            folder.mappings[mapping.name] = mapping

    def _parse_source_or_target(self, elem, origin: str) -> Transformation:
        name = elem.get("NAME", "")
        db_type = elem.get("DATABASETYPE", "")
        t = Transformation(name=name, type=f"{origin.capitalize()} Definition", origin=origin)
        t.attributes = dict(elem.attrib)

        field_tag = "SOURCEFIELD" if origin == "source" else "TARGETFIELD"
        for field_elem in elem.findall(field_tag):
            port = Port(
                name=field_elem.get("NAME", ""),
                data_type=field_elem.get("DATATYPE", ""),
                precision=field_elem.get("PRECISION", ""),
                scale=field_elem.get("SCALE", ""),
                porttype=field_elem.get("PORTTYPE", "INPUT/OUTPUT"),   # for source/target it's not explicitly given, but we assume pass‑through
                default_value=field_elem.get("DEFAULTVALUE", ""),
                description=field_elem.get("DESCRIPTION", ""),
                parent_transform_name=name
            )
            t.ports.append(port)
        return t

    def _parse_transformation(self, elem, origin: str = "inline") -> Transformation:
        name = elem.get("NAME", "")
        ttype = elem.get("TYPE", "")
        reusable = elem.get("REUSABLE", "NO") == "YES"
        t = Transformation(name=name, type=ttype, reusable=reusable, origin=origin)
        t.attributes = dict(elem.attrib)

        # Parse ports (TRANSFORMFIELD)
        for field_elem in elem.findall("TRANSFORMFIELD"):
            port = Port(
                name=field_elem.get("NAME", ""),
                data_type=field_elem.get("DATATYPE", ""),
                precision=field_elem.get("PRECISION", ""),
                scale=field_elem.get("SCALE", ""),
                porttype=field_elem.get("PORTTYPE", ""),
                default_value=field_elem.get("DEFAULTVALUE", ""),
                description=field_elem.get("DESCRIPTION", ""),
                expression=field_elem.get("EXPRESSION", ""),
                parent_transform_name=name
            )
            t.ports.append(port)

        # Parse TABLEATTRIBUTE (for things like Lookup Sql Override, Tracing Level, etc.)
        for attr_elem in elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "")
            attr_value = attr_elem.get("VALUE", "")
            t.attributes[attr_name] = attr_value

        return t

    def _parse_mapping(self, map_elem) -> Mapping:
        name = map_elem.get("NAME", "")
        mapping = Mapping(name=name)

        # Parse inline transformations (SOURCE, EXPRESSION, LOOKUP, etc.)
        for trans_elem in map_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="mapping_inline")
            mapping.transformations[t.name] = t

        # Parse INSTANCE elements (reusable source, target, transformations, mapplets)
        # DO NOT overwrite an inline transformation that already has the same name.
        for inst_elem in map_elem.findall("INSTANCE"):
            inst_name = inst_elem.get("NAME", "")
            if inst_name in mapping.transformations:
                continue   # already parsed as inline transformation, keep it
            trans_name = inst_elem.get("TRANSFORMATION_NAME", inst_name)
            trans_type = inst_elem.get("TRANSFORMATION_TYPE", "")
            t = Transformation(name=inst_name, type=trans_type, reusable=True, origin="mapping_instance")
            t.attributes["TRANSFORMATION_NAME"] = trans_name
            mapping.transformations[inst_name] = t

        # Parse connectors (graph edges)
        for conn_elem in map_elem.findall("CONNECTOR"):
            connector = Connector(
                from_instance=conn_elem.get("FROMINSTANCE", ""),
                from_field=conn_elem.get("FROMFIELD", ""),
                from_instance_type=conn_elem.get("FROMINSTANCETYPE", ""),
                to_instance=conn_elem.get("TOINSTANCE", ""),
                to_field=conn_elem.get("TOFIELD", ""),
                to_instance_type=conn_elem.get("TOINSTANCETYPE", ""),
            )
            mapping.connectors.append(connector)

        # Mapping variables
        for var_elem in map_elem.findall("MAPPINGVARIABLE"):
            var_name = var_elem.get("NAME", "")
            var_default = var_elem.get("DEFAULTVALUE", "")
            mapping.variables[var_name] = var_default

        return mapping

    def _parse_mapplet(self, mpl_elem) -> Mapplet:
        name = mpl_elem.get("NAME", "")
        mpl = Mapplet(name=name)

        # Inline transformations
        for trans_elem in mpl_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="mapplet_inline")
            mpl.transformations[t.name] = t

        # Instances - do not overwrite
        for inst_elem in mpl_elem.findall("INSTANCE"):
            inst_name = inst_elem.get("NAME", "")
            if inst_name in mpl.transformations:
                continue
            trans_name = inst_elem.get("TRANSFORMATION_NAME", inst_name)
            trans_type = inst_elem.get("TRANSFORMATION_TYPE", "")
            t = Transformation(name=inst_name, type=trans_type, reusable=True, origin="mapplet_instance")
            t.attributes["TRANSFORMATION_NAME"] = trans_name
            mpl.transformations[inst_name] = t

        # Connectors
        for conn_elem in mpl_elem.findall("CONNECTOR"):
            connector = Connector(
                from_instance=conn_elem.get("FROMINSTANCE", ""),
                from_field=conn_elem.get("FROMFIELD", ""),
                from_instance_type=conn_elem.get("FROMINSTANCETYPE", ""),
                to_instance=conn_elem.get("TOINSTANCE", ""),
                to_field=conn_elem.get("TOFIELD", ""),
                to_instance_type=conn_elem.get("TOINSTANCETYPE", ""),
            )
            mpl.connectors.append(connector)

        # Mapping variables
        for var_elem in mpl_elem.findall("MAPPINGVARIABLE"):
            var_name = var_elem.get("NAME", "")
            var_default = var_elem.get("DEFAULTVALUE", "")
            mpl.variables[var_name] = var_default

        return mpl

    # ------------------------------------------------------------------
    # Resolve connections – add source_transform/source_port to each port
    # Also copy attributes from reusable definitions to instances.
    # ------------------------------------------------------------------
    def resolve_connections_in_mapping(self, mapping: Mapping, folder: Folder):
        """
        Using the mapping's connectors, annotate each port with its upstream source.
        This assumes that the target ports are already loaded (from instances definitions).
        For instances that are sources/targets/reusable, we need to look up the actual port definitions
        from the folder's sources/targets/reusable_transformations, and also copy attributes.
        """
        # First, for each instance, if it's a source or target, fetch its real ports and attributes from folder definitions
        for inst_name, inst_trans in mapping.transformations.items():
            if inst_trans.origin == "mapping_instance":
                trans_name = inst_trans.attributes.get("TRANSFORMATION_NAME", "")
                ttype = inst_trans.type
                real_trans = None
                if ttype == "Source Definition":
                    real_trans = folder.sources.get(trans_name)
                elif ttype == "Target Definition":
                    real_trans = folder.targets.get(trans_name)
                elif ttype in ("Expression", "Lookup", "Joiner", "Filter", "Router", "Aggregator", "Sequence", "Sorter", "Union", "Normalizer", "SQL Transformation", "Stored Procedure", "Custom Transformation", "Mapplet"):
                    real_trans = folder.reusable_transformations.get(trans_name)
                    if not real_trans:
                        # Maybe it's a mapplet instance? We'll treat it as a black box for now.
                        pass

                if real_trans:
                    # Copy ports from the definition into the instance
                    inst_trans.ports = []
                    for port in real_trans.ports:
                        new_port = Port(
                            name=port.name,
                            data_type=port.data_type,
                            precision=port.precision,
                            scale=port.scale,
                            porttype=port.porttype,
                            default_value=port.default_value,
                            expression=port.expression,
                            parent_transform_name=inst_name,
                        )
                        inst_trans.ports.append(new_port)
                    # Copy attributes from the reusable definition to the instance
                    inst_trans.attributes.update(real_trans.attributes)

        # Now apply connectors: for each connector, find the target port and set its source
        for conn in mapping.connectors:
            target_trans = mapping.transformations.get(conn.to_instance)
            if not target_trans:
                continue
            for port in target_trans.ports:
                if port.name == conn.to_field:
                    port.source_transform = conn.from_instance
                    port.source_port = conn.from_field
                    break

    # Similarly for mapplets
    def resolve_connections_in_mapplet(self, mapplet: Mapplet, folder: Folder):
        # First, for each instance, fetch real transformation and copy ports + attributes
        for inst_name, inst_trans in mapplet.transformations.items():
            if inst_trans.origin == "mapplet_instance":
                trans_name = inst_trans.attributes.get("TRANSFORMATION_NAME", "")
                ttype = inst_trans.type
                real_trans = None
                if ttype == "Source Definition":
                    real_trans = folder.sources.get(trans_name)
                elif ttype == "Target Definition":
                    real_trans = folder.targets.get(trans_name)
                elif ttype in ("Expression", "Lookup", "Joiner", "Filter", "Router", "Aggregator", "Sequence", "Sorter", "Union", "Normalizer", "SQL Transformation", "Stored Procedure", "Custom Transformation", "Mapplet"):
                    real_trans = folder.reusable_transformations.get(trans_name)
                if real_trans:
                    inst_trans.ports = []
                    for port in real_trans.ports:
                        new_port = Port(
                            name=port.name,
                            data_type=port.data_type,
                            precision=port.precision,
                            scale=port.scale,
                            porttype=port.porttype,
                            default_value=port.default_value,
                            expression=port.expression,
                            parent_transform_name=inst_name,
                        )
                        inst_trans.ports.append(new_port)
                    # Copy attributes from reusable definition
                    inst_trans.attributes.update(real_trans.attributes)

        # Now resolve connectors
        for conn in mapplet.connectors:
            target_trans = mapplet.transformations.get(conn.to_instance)
            if not target_trans:
                continue
            for port in target_trans.ports:
                if port.name == conn.to_field:
                    port.source_transform = conn.from_instance
                    port.source_port = conn.from_field
                    break

    def to_json(self) -> str:
        """Serialize repository to JSON for inspection."""
        return json.dumps(self.repo, default=lambda o: o.__dict__, indent=2, ensure_ascii=False)


# ----------------------------------------------------------------------
# Quick test if run directly
# ----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python powercenter_parser.py <xml_file>")
        sys.exit(1)

    parser = PowerCenterParser()
    repo = parser.parse(sys.argv[1])

    # Example: resolve connections for all mappings in first folder
    for folder_name, folder in repo.folders.items():
        print(f"\nFolder: {folder_name}")
        for map_name, mapping in folder.mappings.items():
            parser.resolve_connections_in_mapping(mapping, folder)
            print(f"  Mapping: {map_name}")
            for inst_name, t in mapping.transformations.items():
                print(f"    Transformation: {inst_name} [{t.type}]")
                for p in t.ports:
                    src = f" <- {p.source_transform}.{p.source_port}" if p.source_transform else ""
                    expr = f" expr={p.expression}" if p.expression else ""
                    print(f"      Port: {p.name} ({p.data_type}) {p.porttype}{src}{expr}")

    # Save full JSON representation
    with open("output.json", "w", encoding="utf-8") as f:
        f.write(parser.to_json())
    print("\nFull JSON written to output.json")