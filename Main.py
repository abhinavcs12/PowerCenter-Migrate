from powercenter_parser import PowerCenterXMLParser

parser = PowerCenterXMLParser()  # uses default SCHEMA_MAP, or pass your custom dict
repo = parser.parse("your_export.xml")

# Inspect a mapping
for folder_name, content in repo.folders.items():
    print(f"Folder: {folder_name}")
    for map_name, mapping in content['mappings'].items():
        print(f"  Mapping: {map_name}")
        for t in mapping.transformations:
            print(f"    Transformation: {t.name} [{t.type}]")
            for p in t.ports:
                conn = ""
                if p.connected_to_transformation:
                    conn = f" <- {p.connected_to_transformation}.{p.connected_to_port}"
                print(f"      Port: {p.name} ({p.data_type}) {conn}")
    for wf in content['workflows']:
        print(f"  Workflow: {wf.name}")
        for sess in wf.sessions:
            print(f"    Session: {sess.name} runs mapping {sess.mapping_name}")

# Optionally export full structure as JSON
with open('output.json', 'w') as f:
    f.write(parser.to_json())