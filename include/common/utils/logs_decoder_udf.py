import json

from include.common.constants.index import PROJECT_ID
from include.common.utils.file_helpers import load_json_file

def decode_logs_udf(dataset_id, abi_file_path):
    parsed_abi_components = parse_abi(abi_file_path)

    abi = parsed_abi_components['abi']
    event_name = parsed_abi_components['event_name']
    schema = parsed_abi_components['schema']
    
    return  f"""
        CREATE OR REPLACE FUNCTION 
        `{PROJECT_ID}.{dataset_id}.decode_{event_name}` (log_data STRING, topics ARRAY<STRING>)
        RETURNS STRUCT<{schema}>
        LANGUAGE js
        OPTIONS (library=["gs://blockchain-etl-bigquery/ethers.js"]) AS
        '''
        var abi = [{abi}]
        var interface_instance = new ethers.utils.Interface(abi);
        try {{
           var parsedLog = interface_instance.parseLog({{topics: topics, data: log_data}});
        }}
        catch (e) {{
            return null;
        }}

        var transformParams = function(params, abiInputs) {{
            var result = {{}};
            if (params && params.length >= abiInputs.length) {{
                for (var i = 0; i < abiInputs.length; i++) {{
                    var paramName = abiInputs[i].name;
                    var paramValue = params[i];
                    if (abiInputs[i].type === 'address' && typeof paramValue === 'string') {{
                        // For consistency all addresses are lower-cased.
                        paramValue = paramValue.toLowerCase();
                    }}
                    if (ethers.utils.Interface.isIndexed(paramValue)) {{
                        paramValue = paramValue.hash;
                    }}
                    if (abiInputs[i].type === 'tuple' && 'components' in abiInputs[i]) {{
                        paramValue = transformParams(paramValue, abiInputs[i].components)
                    }}
                    result[paramName] = paramValue;
                }}
            }}
            return result;
        }};

        var result = transformParams(parsedLog.values, abi.inputs);
        return result;
        ''';
    """

def parse_abi(file_path):
    data = load_json_file(file_path)
    abi = data['parser']['abi']

    event_name = abi['name'].lower()
    formatted_abi = json.dumps(abi)
    schema = ', '.join([ f"{column['name']} {column['type']}" for column in data['table']['schema']])

    return {"abi": formatted_abi, "event_name":event_name, "schema":schema}