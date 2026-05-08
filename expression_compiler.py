"""
expression_compiler.py
Translates Informatica expressions into PySpark Column expressions.
Handles multi‑line expressions, string concatenation ||, AND/OR/NOT, NULL,
and falls back to F.expr() when translation fails (without breaking syntax).
"""

from pyspark.sql import functions as F
import re
import sys
from typing import List, Dict, Any

# ----------------------------------------------------------------------
# Pre‑process: clean expression and replace :LKP calls
# ----------------------------------------------------------------------
def preprocess_expression(expr: str) -> str:
    """Normalize whitespace, remove newlines, and replace :LKP placeholder."""
    # Replace all whitespace (newlines, tabs, multiple spaces) with a single space
    expr = re.sub(r'\s+', ' ', expr).strip()
    # Replace :LKP.InstanceName(...) with a placeholder column name
    expr = re.sub(r':LKP\.(\w+)\([^)]*\)', r'\1_OUT', expr)
    return expr

# ----------------------------------------------------------------------
# Tokenizer – simple and robust
# ----------------------------------------------------------------------
def tokenize(expr: str) -> List[str]:
    # First, ensure operators are separated by spaces
    expr = re.sub(r'([()+,=<>!]|&&|\|\|)', r' \1 ', expr)
    # Split by whitespace and filter empty strings
    tokens = [t for t in expr.split() if t]
    # Unquote string literals
    result = []
    for t in tokens:
        if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
            t = t[1:-1]
        result.append(t)
    return result

# ----------------------------------------------------------------------
# AST Nodes
# ----------------------------------------------------------------------
class Literal:
    def __init__(self, value):
        self.value = value

class ColumnRef:
    def __init__(self, name):
        self.name = name

class Variable:
    def __init__(self, name):
        self.name = name

class BinOp:
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

class FuncCall:
    def __init__(self, name, args):
        self.name = name.upper()
        self.args = args

class Null:
    pass

# ----------------------------------------------------------------------
# Recursive descent parser
# ----------------------------------------------------------------------
class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def consume(self, expected=None):
        tok = self.peek()
        if tok is None:
            raise SyntaxError("Unexpected end of expression")
        if expected and tok != expected:
            raise SyntaxError(f"Expected {expected}, got {tok}")
        self.pos += 1
        return tok

    def parse(self):
        return self.logical_or()

    def logical_or(self):
        left = self.logical_and()
        while self.peek() == 'OR':
            self.consume()
            right = self.logical_and()
            left = BinOp(left, 'OR', right)
        return left

    def logical_and(self):
        left = self.comparison()
        while self.peek() == 'AND':
            self.consume()
            right = self.comparison()
            left = BinOp(left, 'AND', right)
        return left

    def comparison(self):
        left = self.string_concat()
        op = self.peek()
        if op in ('<', '<=', '>', '>=', '=', '==', '!=', '<>'):
            self.consume()
            right = self.string_concat()
            if op == '=':
                op = '=='
            elif op == '<>':
                op = '!='
            left = BinOp(left, op, right)
        return left

    def string_concat(self):
        left = self.addsub()
        while self.peek() == '||':
            self.consume()
            right = self.addsub()
            left = BinOp(left, '||', right)
        return left

    def addsub(self):
        left = self.muldiv()
        while self.peek() in ('+', '-'):
            op = self.consume()
            right = self.muldiv()
            left = BinOp(left, op, right)
        return left

    def muldiv(self):
        left = self.unary()
        while self.peek() in ('*', '/'):
            op = self.consume()
            right = self.unary()
            left = BinOp(left, op, right)
        return left

    def unary(self):
        if self.peek() == '-':
            self.consume()
            operand = self.unary()
            return BinOp(Literal(0), '-', operand)
        if self.peek() == 'NOT':
            self.consume()
            operand = self.unary()
            return FuncCall('NOT', [operand])
        return self.primary()

    def primary(self):
        tok = self.peek()
        if tok is None:
            raise SyntaxError("Unexpected end of expression")
        if tok == '(':
            self.consume()
            node = self.logical_or()
            self.consume(')')
            return node
        if tok == 'NULL':
            self.consume()
            return Null()
        # function call: NAME '(' ...
        if (re.match(r'^[A-Za-z_]\w*$', tok) and
            self.pos + 1 < len(self.tokens) and
            self.tokens[self.pos + 1] == '('):
            return self.function_call()
        # variable: $$...
        if re.match(r'^\$\$', tok):
            self.consume()
            return Variable(tok)
        # column reference
        if re.match(r'^[A-Za-z_]\w*$', tok):
            self.consume()
            return ColumnRef(tok)
        # number literal
        if re.match(r'^-?\d+\.?\d*$', tok):
            self.consume()
            try:
                val = float(tok) if '.' in tok else int(tok)
            except ValueError:
                raise SyntaxError(f"Invalid number: {tok}")
            return Literal(val)
        # string literal (already unquoted)
        if tok.startswith("'") or tok.startswith('"'):
            raise SyntaxError("Unexpected token, quotes should be stripped")
        raise SyntaxError(f"Unexpected token: {tok}")

    def function_call(self):
        func_name = self.consume()
        self.consume('(')
        args = []
        if self.peek() != ')':
            args.append(self.logical_or())
            while self.peek() == ',':
                self.consume(',')
                args.append(self.logical_or())
        self.consume(')')
        return FuncCall(func_name, args)

# ----------------------------------------------------------------------
# PySpark Emitter (string returning)
# ----------------------------------------------------------------------
def compile_to_pyspark_str(ast_node, context: Dict[str, Any] = None) -> str:
    if context is None:
        context = {}

    if isinstance(ast_node, Null):
        return 'F.lit(None)'
    elif isinstance(ast_node, Literal):
        val = ast_node.value
        if isinstance(val, str):
            safe_val = val.replace('"""', '\\"\\"\\"')
            return f'F.lit("""{safe_val}""")'
        else:
            return f'F.lit({val})'
    elif isinstance(ast_node, ColumnRef):
        return f'F.col("{ast_node.name}")'
    elif isinstance(ast_node, Variable):
        var_name = ast_node.name
        val = context.get(var_name, None)
        if val is None:
            return 'F.lit(None)'
        if isinstance(val, str):
            safe_val = val.replace('"""', '\\"\\"\\"')
            return f'F.lit("""{safe_val}""")'
        else:
            return f'F.lit({val})'
    elif isinstance(ast_node, BinOp):
        left = compile_to_pyspark_str(ast_node.left, context)
        right = compile_to_pyspark_str(ast_node.right, context)
        op = ast_node.op
        if op in ('+', '-', '*', '/'):
            return f'({left} {op} {right})'
        elif op in ('==', '!=', '<', '<=', '>', '>='):
            return f'({left} {_pyspark_op(op)} {right})'
        elif op == '||':
            return f'F.concat({left}, {right})'
        elif op == 'AND':
            return f'({left} & {right})'
        elif op == 'OR':
            return f'({left} | {right})'
        else:
            raise ValueError(f"Unsupported binary operator: {op}")
    elif isinstance(ast_node, FuncCall):
        func = ast_node.name
        args = [compile_to_pyspark_str(a, context) for a in ast_node.args]
        return _translate_function(func, args)
    else:
        raise ValueError(f"Unknown AST node: {type(ast_node)}")

def _pyspark_op(infa_op):
    mapping = {'==': '==', '!=': '!='}
    return mapping.get(infa_op, infa_op)

def _translate_function(func_name, arg_strs):
    fname = func_name.upper()
    if fname == 'NOT':
        return f'~({arg_strs[0]})'
    elif fname == 'IIF':
        if len(arg_strs) != 3:
            raise ValueError(f"IIF expects 3 arguments, got {len(arg_strs)}")
        cond, tv, fv = arg_strs
        return f'F.when({cond}, {tv}).otherwise({fv})'
    elif fname == 'ISNULL':
        return f'{arg_strs[0]}.isNull()'
    elif fname == 'IS_DATE':
        return f'F.to_date({arg_strs[0]}, "yyyy-MM-dd").isNotNull()'
    elif fname == 'TO_DATE':
        if len(arg_strs) == 1:
            return f'F.to_date({arg_strs[0]})'
        else:
            return f'F.to_date({arg_strs[0]}, {arg_strs[1]})'
    elif fname == 'TO_INTEGER':
        return f'{arg_strs[0]}.cast("int")'
    elif fname == 'TO_FLOAT':
        return f'{arg_strs[0]}.cast("float")'
    elif fname == 'TO_CHAR':
        if len(arg_strs) == 1:
            return f'{arg_strs[0]}.cast("string")'
        else:
            return f'F.date_format({arg_strs[0]}, {arg_strs[1]})'
    elif fname == 'DECODE':
        base = arg_strs[0]
        rest = arg_strs[1:]
        default = 'F.lit(None)'
        if len(rest) % 2 == 1:
            default = rest[-1]
            rest = rest[:-1]
        chain = default
        for i in range(len(rest)-1, -1, -2):
            if i-1 >= 0:
                val = rest[i-1]
                res = rest[i]
                chain = f'F.when({base} == {val}, {res}).otherwise({chain})'
        return chain
    elif fname == 'UPPER':
        return f'F.upper({arg_strs[0]})'
    elif fname == 'LOWER':
        return f'F.lower({arg_strs[0]})'
    elif fname == 'TRIM':
        return f'F.trim({arg_strs[0]})'
    elif fname == 'LTRIM':
        return f'F.ltrim({arg_strs[0]})'
    elif fname == 'RTRIM':
        return f'F.rtrim({arg_strs[0]})'
    elif fname in ('SUBSTR', 'SUBSTRING'):
        if len(arg_strs) == 2:
            return f'F.substring({arg_strs[0]}, {arg_strs[1]}, F.length({arg_strs[0]}))'
        else:
            return f'F.substring({arg_strs[0]}, {arg_strs[1]}, {arg_strs[2]})'
    elif fname == 'LENGTH':
        return f'F.length({arg_strs[0]})'
    elif fname == 'CONCAT':
        return f'F.concat({", ".join(arg_strs)})'
    elif fname in ('NVL', 'COALESCE'):
        return f'F.coalesce({", ".join(arg_strs)})'
    elif fname == 'SYSDATE':
        return 'F.current_timestamp()'
    else:
        # fallback: use SQL expression
        args_concat = ", ".join(arg_strs)
        return f'F.expr("{fname}(" + {args_concat} + ")")'

def translate_expression(expr_str: str, variables: Dict[str, Any] = None) -> str:
    """
    Translate an Informatica expression to a PySpark Column expression string.
    If parsing fails, print error and return F.expr(original) without breaking syntax.
    """
    try:
        expr_str = preprocess_expression(expr_str)
        tokens = tokenize(expr_str)
        parser = Parser(tokens)
        ast = parser.parse()
        return compile_to_pyspark_str(ast, variables)
    except Exception as e:
        # Print error to stderr for debugging, but return a clean F.expr for the generated code
        print(f"ERROR translating expression: {expr_str[:200]}...", file=sys.stderr)
        print(f"  Reason: {e}", file=sys.stderr)
        # Escape triple quotes inside the expression
        safe_expr = expr_str.replace('"""', '\\"\\"\\"')
        return f'F.expr("""{safe_expr}""")'
        
# """
# expression_compiler.py
# Translates Informatica expressions into PySpark Column expressions.
# Handles multi‑line expressions, string concatenation ||, AND/OR/NOT, NULL,
# and falls back to F.expr() when translation fails.
# """

# from pyspark.sql import functions as F
# import re
# from typing import List, Dict, Any

# # ----------------------------------------------------------------------
# # Pre‑process: clean expression and replace :LKP calls
# # ----------------------------------------------------------------------
# def preprocess_expression(expr: str) -> str:
#     """Normalize whitespace, remove newlines, and replace :LKP placeholder."""
#     # Replace all whitespace (newlines, tabs, multiple spaces) with a single space
#     expr = re.sub(r'\s+', ' ', expr).strip()
#     # Replace :LKP.InstanceName(...) with a placeholder column name
#     expr = re.sub(r':LKP\.(\w+)\([^)]*\)', r'\1_OUT', expr)
#     return expr

# # ----------------------------------------------------------------------
# # Tokenizer – simple and robust
# # ----------------------------------------------------------------------
# # We split by whitespace then classify tokens. This avoids complex regex.
# def tokenize(expr: str) -> List[str]:
#     # First, ensure operators are separated by spaces
#     # Add spaces around known operators and parentheses
#     expr = re.sub(r'([()+,=<>!]|&&|\|\|)', r' \1 ', expr)
#     # Split by whitespace and filter empty strings
#     tokens = [t for t in expr.split() if t]
#     # Unquote string literals
#     result = []
#     for t in tokens:
#         if (t.startswith("'") and t.endswith("'")) or (t.startswith('"') and t.endswith('"')):
#             t = t[1:-1]
#         result.append(t)
#     return result

# # ----------------------------------------------------------------------
# # AST Nodes
# # ----------------------------------------------------------------------
# class Literal:
#     def __init__(self, value):
#         self.value = value

# class ColumnRef:
#     def __init__(self, name):
#         self.name = name

# class Variable:
#     def __init__(self, name):
#         self.name = name

# class BinOp:
#     def __init__(self, left, op, right):
#         self.left = left
#         self.op = op
#         self.right = right

# class FuncCall:
#     def __init__(self, name, args):
#         self.name = name.upper()
#         self.args = args

# class Null:
#     pass

# # ----------------------------------------------------------------------
# # Recursive descent parser
# # ----------------------------------------------------------------------
# class Parser:
#     def __init__(self, tokens):
#         self.tokens = tokens
#         self.pos = 0

#     def peek(self):
#         return self.tokens[self.pos] if self.pos < len(self.tokens) else None

#     def consume(self, expected=None):
#         tok = self.peek()
#         if tok is None:
#             raise SyntaxError("Unexpected end of expression")
#         if expected and tok != expected:
#             raise SyntaxError(f"Expected {expected}, got {tok}")
#         self.pos += 1
#         return tok

#     def parse(self):
#         return self.logical_or()

#     # logical_or = logical_and ('OR' logical_and)*
#     def logical_or(self):
#         left = self.logical_and()
#         while self.peek() == 'OR':
#             self.consume()
#             right = self.logical_and()
#             left = BinOp(left, 'OR', right)
#         return left

#     # logical_and = comparison ('AND' comparison)*
#     def logical_and(self):
#         left = self.comparison()
#         while self.peek() == 'AND':
#             self.consume()
#             right = self.comparison()
#             left = BinOp(left, 'AND', right)
#         return left

#     # comparison = string_concat (op string_concat)?
#     def comparison(self):
#         left = self.string_concat()
#         op = self.peek()
#         if op in ('<', '<=', '>', '>=', '=', '==', '!=', '<>'):
#             self.consume()
#             right = self.string_concat()
#             if op == '=':
#                 op = '=='
#             elif op == '<>':
#                 op = '!='
#             left = BinOp(left, op, right)
#         return left

#     # string_concat = addsub ('||' addsub)*
#     def string_concat(self):
#         left = self.addsub()
#         while self.peek() == '||':
#             self.consume()
#             right = self.addsub()
#             left = BinOp(left, '||', right)
#         return left

#     # addsub = muldiv ( ('+'|'-') muldiv )*
#     def addsub(self):
#         left = self.muldiv()
#         while self.peek() in ('+', '-',):
#             op = self.consume()
#             right = self.muldiv()
#             left = BinOp(left, op, right)
#         return left

#     # muldiv = unary ( ('*'|'/') unary )*
#     def muldiv(self):
#         left = self.unary()
#         while self.peek() in ('*', '/'):
#             op = self.consume()
#             right = self.unary()
#             left = BinOp(left, op, right)
#         return left

#     # unary = ('-'|'NOT')? primary
#     def unary(self):
#         if self.peek() == '-':
#             self.consume()
#             operand = self.unary()
#             return BinOp(Literal(0), '-', operand)
#         if self.peek() == 'NOT':
#             self.consume()
#             operand = self.unary()
#             return FuncCall('NOT', [operand])
#         return self.primary()

#     # primary = '(' expr ')' | NULL | literal | variable | column_ref | function_call
#     def primary(self):
#         tok = self.peek()
#         if tok is None:
#             raise SyntaxError("Unexpected end of expression")
#         if tok == '(':
#             self.consume()
#             node = self.logical_or()
#             self.consume(')')
#             return node
#         if tok == 'NULL':
#             self.consume()
#             return Null()
#         # function call: NAME '(' ...
#         if (re.match(r'^[A-Za-z_]\w*$', tok) and
#             self.pos + 1 < len(self.tokens) and
#             self.tokens[self.pos + 1] == '('):
#             return self.function_call()
#         # variable: $$...
#         if re.match(r'^\$\$', tok):
#             self.consume()
#             return Variable(tok)
#         # column reference
#         if re.match(r'^[A-Za-z_]\w*$', tok):
#             self.consume()
#             return ColumnRef(tok)
#         # number literal
#         if re.match(r'^-?\d+\.?\d*$', tok):
#             self.consume()
#             try:
#                 val = float(tok) if '.' in tok else int(tok)
#             except ValueError:
#                 raise SyntaxError(f"Invalid number: {tok}")
#             return Literal(val)
#         # string literal (already unquoted)
#         if tok.startswith("'") or tok.startswith('"'):
#             raise SyntaxError("Unexpected token, quotes should be stripped")
#         raise SyntaxError(f"Unexpected token: {tok}")

#     def function_call(self):
#         func_name = self.consume()
#         self.consume('(')
#         args = []
#         if self.peek() != ')':
#             args.append(self.logical_or())
#             while self.peek() == ',':
#                 self.consume(',')
#                 args.append(self.logical_or())
#         self.consume(')')
#         return FuncCall(func_name, args)

# # ----------------------------------------------------------------------
# # PySpark Emitter (string returning)
# # ----------------------------------------------------------------------
# def compile_to_pyspark_str(ast_node, context: Dict[str, Any] = None) -> str:
#     if context is None:
#         context = {}

#     if isinstance(ast_node, Null):
#         return 'F.lit(None)'
#     elif isinstance(ast_node, Literal):
#         val = ast_node.value
#         if isinstance(val, str):
#             safe_val = val.replace('"""', '\\"\\"\\"')
#             return f'F.lit("""{safe_val}""")'
#         else:
#             return f'F.lit({val})'
#     elif isinstance(ast_node, ColumnRef):
#         return f'F.col("{ast_node.name}")'
#     elif isinstance(ast_node, Variable):
#         var_name = ast_node.name
#         val = context.get(var_name, None)
#         if val is None:
#             return 'F.lit(None)'
#         if isinstance(val, str):
#             safe_val = val.replace('"""', '\\"\\"\\"')
#             return f'F.lit("""{safe_val}""")'
#         else:
#             return f'F.lit({val})'
#     elif isinstance(ast_node, BinOp):
#         left = compile_to_pyspark_str(ast_node.left, context)
#         right = compile_to_pyspark_str(ast_node.right, context)
#         op = ast_node.op
#         if op in ('+', '-', '*', '/'):
#             return f'({left} {op} {right})'
#         elif op in ('==', '!=', '<', '<=', '>', '>='):
#             return f'({left} {_pyspark_op(op)} {right})'
#         elif op == '||':
#             return f'F.concat({left}, {right})'
#         elif op == 'AND':
#             return f'({left} & {right})'
#         elif op == 'OR':
#             return f'({left} | {right})'
#         else:
#             raise ValueError(f"Unsupported binary operator: {op}")
#     elif isinstance(ast_node, FuncCall):
#         func = ast_node.name
#         args = [compile_to_pyspark_str(a, context) for a in ast_node.args]
#         return _translate_function(func, args)
#     else:
#         raise ValueError(f"Unknown AST node: {type(ast_node)}")

# def _pyspark_op(infa_op):
#     mapping = {'==': '==', '!=': '!='}
#     return mapping.get(infa_op, infa_op)

# def _translate_function(func_name, arg_strs):
#     fname = func_name.upper()
#     if fname == 'NOT':
#         return f'~({arg_strs[0]})'
#     elif fname == 'IIF':
#         if len(arg_strs) != 3:
#             raise ValueError(f"IIF expects 3 arguments, got {len(arg_strs)}")
#         cond, tv, fv = arg_strs
#         return f'F.when({cond}, {tv}).otherwise({fv})'
#     elif fname == 'ISNULL':
#         return f'{arg_strs[0]}.isNull()'
#     elif fname == 'IS_DATE':
#         return f'F.to_date({arg_strs[0]}, "yyyy-MM-dd").isNotNull()'
#     elif fname == 'TO_DATE':
#         if len(arg_strs) == 1:
#             return f'F.to_date({arg_strs[0]})'
#         else:
#             return f'F.to_date({arg_strs[0]}, {arg_strs[1]})'
#     elif fname == 'TO_INTEGER':
#         return f'{arg_strs[0]}.cast("int")'
#     elif fname == 'TO_FLOAT':
#         return f'{arg_strs[0]}.cast("float")'
#     elif fname == 'TO_CHAR':
#         if len(arg_strs) == 1:
#             return f'{arg_strs[0]}.cast("string")'
#         else:
#             return f'F.date_format({arg_strs[0]}, {arg_strs[1]})'
#     elif fname == 'DECODE':
#         base = arg_strs[0]
#         rest = arg_strs[1:]
#         default = 'F.lit(None)'
#         if len(rest) % 2 == 1:
#             default = rest[-1]
#             rest = rest[:-1]
#         chain = default
#         for i in range(len(rest)-1, -1, -2):
#             if i-1 >= 0:
#                 val = rest[i-1]
#                 res = rest[i]
#                 chain = f'F.when({base} == {val}, {res}).otherwise({chain})'
#         return chain
#     elif fname == 'UPPER':
#         return f'F.upper({arg_strs[0]})'
#     elif fname == 'LOWER':
#         return f'F.lower({arg_strs[0]})'
#     elif fname == 'TRIM':
#         return f'F.trim({arg_strs[0]})'
#     elif fname == 'LTRIM':
#         return f'F.ltrim({arg_strs[0]})'
#     elif fname == 'RTRIM':
#         return f'F.rtrim({arg_strs[0]})'
#     elif fname in ('SUBSTR', 'SUBSTRING'):
#         if len(arg_strs) == 2:
#             return f'F.substring({arg_strs[0]}, {arg_strs[1]}, F.length({arg_strs[0]}))'
#         else:
#             return f'F.substring({arg_strs[0]}, {arg_strs[1]}, {arg_strs[2]})'
#     elif fname == 'LENGTH':
#         return f'F.length({arg_strs[0]})'
#     elif fname == 'CONCAT':
#         return f'F.concat({", ".join(arg_strs)})'
#     elif fname in ('NVL', 'COALESCE'):
#         return f'F.coalesce({", ".join(arg_strs)})'
#     elif fname == 'SYSDATE':
#         return 'F.current_timestamp()'
#     else:
#         # fallback: use SQL expression
#         args_concat = ", ".join(arg_strs)
#         return f'F.expr("{fname}(" + {args_concat} + ")")'

# def translate_expression(expr_str: str, variables: Dict[str, Any] = None) -> str:
#     """
#     Translate an Informatica expression to a PySpark Column expression string.
#     If parsing fails, return F.expr(original) so that the generated script still runs.
#     """
#     try:
#         expr_str = preprocess_expression(expr_str)
#         tokens = tokenize(expr_str)
#         parser = Parser(tokens)
#         ast = parser.parse()
#         return compile_to_pyspark_str(ast, variables)
#     except Exception as e:
#         # Fallback: use raw SQL expression
#         # Ensure any single quotes are escaped for use inside F.expr()
#         safe_expr = expr_str.replace("'", "\\'")
#         return f'F.expr("""{safe_expr}""")  # PARSE ERROR: {e}'

        ##############################

# """
# expression_compiler.py
# Translates Informatica expressions into PySpark Column expressions.
# Handles :LKP.xxx() calls, string concatenation ||, AND/OR/NOT, NULL, and multi-line expressions.
# """

# from pyspark.sql import functions as F, Column
# import re
# from typing import List, Dict, Any

# # ----------------------------------------------------------------------
# # Pre‑process : clean expression string and replace lookup calls
# # ----------------------------------------------------------------------
# def preprocess_expression(expr: str) -> str:
#     """Remove newlines, extra spaces, and replace :LKP calls with a placeholder column."""
#     # Replace newlines and carriage returns with spaces
#     expr = expr.replace('\n', ' ').replace('\r', ' ')
#     # Collapse multiple spaces into one
#     expr = re.sub(r'\s+', ' ', expr).strip()
#     # Replace :LKP.InstanceName(...) with InstanceName_OUT (temporary placeholder)
#     expr = re.sub(r':LKP\.(\w+)\([^)]*\)', r'\1_OUT', expr)
#     return expr

# # ----------------------------------------------------------------------
# # Tokenizer (updated for multi-line and operators)
# # ----------------------------------------------------------------------
# TOKEN_PATTERN = re.compile(r"""
#     '.*?'                     | # single-quoted strings
#     ".*?"                     | # double-quoted strings
#     \$\$[A-Za-z_]\w*          | # mapping variables
#     NULL                      | # NULL keyword
#     AND|OR|NOT                | # logical operators
#     \|\|                      | # string concatenation
#     <=|>=|<>|!=|==            | # comparison operators
#     <|>|=                     |
#     [A-Za-z_]\w*              | # identifiers
#     -?\d+\.?\d*               | # numbers
#     [+\-*/()]                 | # arithmetic operators
#     ,                           # comma
# """, re.VERBOSE)

# def tokenize(expr: str) -> List[str]:
#     tokens = []
#     for match in TOKEN_PATTERN.finditer(expr):
#         token = match.group()
#         # strip quotes from string literals
#         if (token.startswith("'") and token.endswith("'")) or \
#            (token.startswith('"') and token.endswith('"')):
#             token = token[1:-1]
#         tokens.append(token)
#     return tokens

# # ----------------------------------------------------------------------
# # AST Nodes
# # ----------------------------------------------------------------------
# class Literal:
#     def __init__(self, value):
#         self.value = value

# class ColumnRef:
#     def __init__(self, name):
#         self.name = name

# class Variable:
#     def __init__(self, name):
#         self.name = name

# class BinOp:
#     def __init__(self, left, op, right):
#         self.left = left
#         self.op = op
#         self.right = right

# class FuncCall:
#     def __init__(self, name, args):
#         self.name = name.upper()
#         self.args = args

# class Null:
#     pass

# # ----------------------------------------------------------------------
# # Recursive descent parser (improved for multi-line and IIF)
# # ----------------------------------------------------------------------
# class Parser:
#     def __init__(self, tokens):
#         self.tokens = tokens
#         self.pos = 0

#     def peek(self):
#         return self.tokens[self.pos] if self.pos < len(self.tokens) else None

#     def consume(self, expected=None):
#         tok = self.peek()
#         if tok is None:
#             raise SyntaxError("Unexpected end of expression")
#         if expected and tok != expected:
#             raise SyntaxError(f"Expected {expected}, got {tok}")
#         self.pos += 1
#         return tok

#     def parse(self):
#         return self.logical_or()

#     # logical_or = logical_and ('OR' logical_and)*
#     def logical_or(self):
#         left = self.logical_and()
#         while self.peek() == 'OR':
#             self.consume()
#             right = self.logical_and()
#             left = BinOp(left, 'OR', right)
#         return left

#     # logical_and = comparison ('AND' comparison)*
#     def logical_and(self):
#         left = self.comparison()
#         while self.peek() == 'AND':
#             self.consume()
#             right = self.comparison()
#             left = BinOp(left, 'AND', right)
#         return left

#     # comparison = string_concat (op string_concat)?
#     def comparison(self):
#         left = self.string_concat()
#         op = self.peek()
#         if op in ('<', '<=', '>', '>=', '=', '==', '!=', '<>'):
#             self.consume()
#             right = self.string_concat()
#             if op == '=':   # Informatica uses '=' for equality
#                 op = '=='
#             elif op == '<>':
#                 op = '!='
#             left = BinOp(left, op, right)
#         return left

#     # string_concat = addsub ('||' addsub)*
#     def string_concat(self):
#         left = self.addsub()
#         while self.peek() == '||':
#             self.consume()
#             right = self.addsub()
#             left = BinOp(left, '||', right)
#         return left

#     # addsub = muldiv ( ('+'|'-') muldiv )*
#     def addsub(self):
#         left = self.muldiv()
#         while self.peek() in ('+', '-'):
#             op = self.consume()
#             right = self.muldiv()
#             left = BinOp(left, op, right)
#         return left

#     # muldiv = unary ( ('*'|'/') unary )*
#     def muldiv(self):
#         left = self.unary()
#         while self.peek() in ('*', '/'):
#             op = self.consume()
#             right = self.unary()
#             left = BinOp(left, op, right)
#         return left

#     # unary = ('-'|'NOT')? primary
#     def unary(self):
#         if self.peek() == '-':
#             self.consume()
#             operand = self.unary()
#             return BinOp(Literal(0), '-', operand)
#         if self.peek() == 'NOT':
#             self.consume()
#             operand = self.unary()
#             return FuncCall('NOT', [operand])
#         return self.primary()

#     # primary = '(' expr ')' | NULL | literal | variable | column_ref | function_call
#     def primary(self):
#         tok = self.peek()
#         if tok is None:
#             raise SyntaxError("Unexpected end of expression")
#         if tok == '(':
#             self.consume()
#             node = self.logical_or()
#             self.consume(')')
#             return node
#         if tok == 'NULL':
#             self.consume()
#             return Null()
#         # function call: NAME '(' ...
#         # Note: We need to look ahead for '(' after identifier
#         if (re.match(r'^[A-Za-z_]\w*$', tok) and 
#             self.pos + 1 < len(self.tokens) and 
#             self.tokens[self.pos + 1] == '('):
#             return self.function_call()
#         # variable: $$...
#         if re.match(r'^\$\$', tok):
#             self.consume()
#             return Variable(tok)
#         # column reference
#         if re.match(r'^[A-Za-z_]\w*$', tok):
#             self.consume()
#             return ColumnRef(tok)
#         # number literal
#         if re.match(r'^-?\d+\.?\d*$', tok):
#             self.consume()
#             try:
#                 val = float(tok) if '.' in tok else int(tok)
#             except ValueError:
#                 raise SyntaxError(f"Invalid number: {tok}")
#             return Literal(val)
#         # string literal (already unquoted by tokenizer)
#         if tok.startswith("'") or tok.startswith('"'):
#             raise SyntaxError("Unexpected token, quotes should be stripped")
#         raise SyntaxError(f"Unexpected token: {tok}")

#     def function_call(self):
#         func_name = self.consume()
#         self.consume('(')
#         args = []
#         if self.peek() != ')':
#             args.append(self.logical_or())
#             while self.peek() == ',':
#                 self.consume(',')
#                 args.append(self.logical_or())
#         self.consume(')')
#         return FuncCall(func_name, args)

# # ----------------------------------------------------------------------
# # PySpark Emitter (string returning)
# # ----------------------------------------------------------------------
# def compile_to_pyspark_str(ast_node, context: Dict[str, Any] = None) -> str:
#     if context is None:
#         context = {}

#     if isinstance(ast_node, Null):
#         return 'F.lit(None)'
#     elif isinstance(ast_node, Literal):
#         val = ast_node.value
#         if isinstance(val, str):
#             # escape triple quotes inside string literals
#             safe_val = val.replace('"""', '\\"\\"\\"')
#             return f'F.lit("""{safe_val}""")'
#         else:
#             return f'F.lit({val})'
#     elif isinstance(ast_node, ColumnRef):
#         return f'F.col("{ast_node.name}")'
#     elif isinstance(ast_node, Variable):
#         var_name = ast_node.name
#         val = context.get(var_name, None)
#         if val is None:
#             return 'F.lit(None)'
#         if isinstance(val, str):
#             if val.isdigit():
#                 return f'F.lit({val})'
#             else:
#                 safe_val = val.replace('"""', '\\"\\"\\"')
#                 return f'F.lit("""{safe_val}""")'
#         else:
#             return f'F.lit({val})'
#     elif isinstance(ast_node, BinOp):
#         left = compile_to_pyspark_str(ast_node.left, context)
#         right = compile_to_pyspark_str(ast_node.right, context)
#         op = ast_node.op
#         if op in ('+', '-', '*', '/'):
#             return f'({left} {op} {right})'
#         elif op in ('==', '!=', '<', '<=', '>', '>='):
#             return f'({left} {_pyspark_op(op)} {right})'
#         elif op == '||':
#             return f'F.concat({left}, {right})'
#         elif op == 'AND':
#             return f'({left} & {right})'
#         elif op == 'OR':
#             return f'({left} | {right})'
#         else:
#             raise ValueError(f"Unsupported binary operator: {op}")
#     elif isinstance(ast_node, FuncCall):
#         func = ast_node.name
#         args = [compile_to_pyspark_str(a, context) for a in ast_node.args]
#         return _translate_function(func, args)
#     else:
#         raise ValueError(f"Unknown AST node: {type(ast_node)}")

# def _pyspark_op(infa_op):
#     mapping = {'==': '==', '!=': '!='}
#     return mapping.get(infa_op, infa_op)

# def _translate_function(func_name, arg_strs):
#     fname = func_name.upper()
#     if fname == 'NOT':
#         return f'~({arg_strs[0]})'
#     elif fname == 'IIF':
#         # IIF(condition, true_value, false_value)
#         if len(arg_strs) != 3:
#             raise ValueError(f"IIF expects 3 arguments, got {len(arg_strs)}")
#         cond, tv, fv = arg_strs
#         return f'F.when({cond}, {tv}).otherwise({fv})'
#     elif fname == 'ISNULL':
#         return f'{arg_strs[0]}.isNull()'
#     elif fname == 'IS_DATE':
#         return f'F.to_date({arg_strs[0]}, "yyyy-MM-dd").isNotNull()'
#     elif fname == 'TO_DATE':
#         if len(arg_strs) == 1:
#             return f'F.to_date({arg_strs[0]})'
#         else:
#             return f'F.to_date({arg_strs[0]}, {arg_strs[1]})'
#     elif fname == 'TO_INTEGER':
#         return f'{arg_strs[0]}.cast("int")'
#     elif fname == 'TO_FLOAT':
#         return f'{arg_strs[0]}.cast("float")'
#     elif fname == 'TO_CHAR':
#         if len(arg_strs) == 1:
#             return f'{arg_strs[0]}.cast("string")'
#         else:
#             return f'F.date_format({arg_strs[0]}, {arg_strs[1]})'
#     elif fname == 'DECODE':
#         base = arg_strs[0]
#         rest = arg_strs[1:]
#         default = 'F.lit(None)'
#         pairs = []
#         if len(rest) % 2 == 1:
#             default = rest[-1]
#             rest = rest[:-1]
#         chain = default
#         for i in range(len(rest)-1, -1, -2):
#             if i-1 >= 0:
#                 val = rest[i-1]
#                 res = rest[i]
#                 chain = f'F.when({base} == {val}, {res}).otherwise({chain})'
#         return chain
#     elif fname == 'UPPER':
#         return f'F.upper({arg_strs[0]})'
#     elif fname == 'LOWER':
#         return f'F.lower({arg_strs[0]})'
#     elif fname == 'TRIM':
#         return f'F.trim({arg_strs[0]})'
#     elif fname == 'LTRIM':
#         return f'F.ltrim({arg_strs[0]})'
#     elif fname == 'RTRIM':
#         return f'F.rtrim({arg_strs[0]})'
#     elif fname in ('SUBSTR', 'SUBSTRING'):
#         if len(arg_strs) == 2:
#             return f'F.substring({arg_strs[0]}, {arg_strs[1]}, F.length({arg_strs[0]}))'
#         else:
#             return f'F.substring({arg_strs[0]}, {arg_strs[1]}, {arg_strs[2]})'
#     elif fname == 'LENGTH':
#         return f'F.length({arg_strs[0]})'
#     elif fname == 'CONCAT':
#         return f'F.concat({", ".join(arg_strs)})'
#     elif fname in ('NVL', 'COALESCE'):
#         return f'F.coalesce({", ".join(arg_strs)})'
#     elif fname == 'SYSDATE':
#         return 'F.current_timestamp()'
#     else:
#         # fallback: use SQL expression
#         args_concat = ", ".join([f"str({a})" for a in arg_strs])
#         return f'F.expr("{fname}(" + {args_concat} + ")")'

# def translate_expression(expr_str: str, variables: Dict[str, Any] = None) -> str:
#     # Preprocess: clean whitespace and replace :LKP calls
#     expr_str = preprocess_expression(expr_str)
#     tokens = tokenize(expr_str)
#     parser = Parser(tokens)
#     ast = parser.parse()
#     return compile_to_pyspark_str(ast, variables)