from pathlib import Path
from project.server.main.dataclasses_dc import *
def list_file_dumps():
    
    dirpath = Path.cwd()
    files = dirpath.glob("**/*.ndjson")

    files_list=list(files)
    path_output=Path('project/results').resolve()
    print(path_output)

    return(files_list,path_output)


def format_doi(doi_id):
    doi = doi_id.replace('/', '_').replace(':', '_').replace('*','')
    return doi



def split_dump_file():

    list_path =list_file_dumps()[0]
    for path_file in list_path:
        print(path_file)
        with path_file.open( 'r', encoding='utf-8') as f:
                    # Skip the first line empty(all ndjson file begin by the empty line)
            next(f)
            for jsonObj in f:
                jsonstring= json.loads(jsonObj)
                objects = Root.from_dict_custom(jsonstring)
                for obj in objects.data:
                    name_file= format_doi(obj.id)
                    print(name_file)
                    path_result =list_file_dumps()[1]
                    res = open(path_result /"{0:s}.json".format(name_file),mode='w')
                
                    json.dump(obj.to_dict(),res, indent=4)