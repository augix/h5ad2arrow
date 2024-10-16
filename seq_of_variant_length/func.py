import numpy as np
import pyarrow as pa

def create_random_data(n_records, max_length, max_gene_id, np_types):
    for arrow_i in range(n_records):
        length = np.random.randint(0, max_length)
        genes = np.random.randint(0, max_gene_id, size=length)
        exprs = np.random.rand(length) # float
        # correct the type
        arrow_i = np.array([arrow_i], dtype=np_types['arrow_i'])
        genes = np.array(genes, dtype=np_types['genes'])
        exprs = np.array(exprs, dtype=np_types['exprs'])
        data = {
            'arrow_i': [arrow_i],
            'genes': [genes],
            'exprs': [exprs],
        }
        yield data

def create_schema(keys, pa_types):
    '''
    create a schema from a list of dicts
    - list_of_dicts: a list of dicts, each dict is a record
    - each dict have the same set of keys, but the values of the keys can be of different length
    '''
    return pa.schema([
        pa.field(key, pa.list_(pa_types[key])) for key in keys
    ])

def create_arrow_file(output_arrow_fn, list_of_dicts, schema):
    '''
    create an arrow file from a list of dicts
    - list_of_dicts: a list of dicts, each dict is a record
    - each dict have the same set of keys, but the values of the keys can be of different length
    - schema: the schema of the arrow file

    why sequences can have different length:
    - in each record, the value of dict can be a list, and the length of the list can vary from record to record
    '''
    sink = pa.OSFile(output_arrow_fn, 'wb')
    writer = pa.ipc.new_file(sink, schema=schema)
    for record in list_of_dicts:
        record_batch = pa.RecordBatch.from_pydict(record, schema=schema)
        writer.write_batch(record_batch)
    writer.close()
    sink.close()

def get_record_dict(arrow_file, idx):
    with pa.memory_map(arrow_file, 'rb') as f:
        reader = pa.ipc.open_file(f)
        record = reader.get_batch(idx)
        dict = record.to_pydict()
        return dict