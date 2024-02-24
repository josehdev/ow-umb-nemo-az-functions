import azure.functions as func

from process_manifest import function_process_manifest
#from validate_manifest_old import old_function_validate_manifest

app = func.FunctionApp()
app.register_functions(function_process_manifest)
#app.register_functions(old_function_validate_manifest)
