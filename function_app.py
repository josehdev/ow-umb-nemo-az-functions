"""
 This is the pre-defined centralized file to define Azure Functions with Python.
 In this case, each function's logic is contained in a separate file (Blueprint)
 Those functions (one or many) are registered in this file. 
"""

import azure.functions as func

from process_manifest import function_process_manifest

app = func.FunctionApp()
app.register_functions(function_process_manifest)

