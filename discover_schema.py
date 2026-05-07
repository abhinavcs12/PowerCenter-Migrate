# discover_schema.py

from lxml import etree
import sys

def discover_paths(xml_file, max_depth=6):
    """Print unique element paths and example attributes up to max_depth."""
    seen = set()
    for event, elem in etree.iterparse(xml_file, events=('end',)):
        # Build path from root to this element
        path_parts = []
        parent = elem.getparent()
        while parent is not None:
            # Get tag without namespace
            tag = etree.QName(parent).localname
            # Could add positional index if many siblings
            path_parts.append(tag)
            parent = parent.getparent()
        path_parts.reverse()
        path_parts.append(etree.QName(elem).localname)
        path = '/' + '/'.join(path_parts)

        if path not in seen and len(path_parts) <= max_depth:
            seen.add(path)
            # Print attributes and a snippet of text
            attrs = dict(elem.attrib)
            text = (elem.text or '').strip()[:80]
            print(f"PATH: {path}")
            if attrs:
                print(f"  ATTRIBUTES: {attrs}")
            if text:
                print(f"  TEXT: {text}")
            print()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python discover_schema.py <xml_file>")
        sys.exit(1)
    discover_paths(sys.argv[1])