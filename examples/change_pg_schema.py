import codecs
def replace_pg_schema(template_filename, output_filename, orig_schema, new_schema):
    o_schema_reference = "table='[{}".format(orig_schema)
    n_schema_reference = "table='[{}".format(new_schema)
    # The file needs to be opened as string so that String methods can be used to read each line
    with codecs.open(template_filename, 'r', encoding='utf-8') as ds_fh:
        with codecs.open(output_filename, 'w', encoding='utf-8') as outfile:
            for line in ds_fh:
                if line.find(o_schema_reference) != -1:
                    new_line = line.replace(o_schema_reference, n_schema_reference)
                    outfile.write(new_line)
                else:
                    outfile.write(line)
    print('Changed DS saved to: {}'.format(output_filename))