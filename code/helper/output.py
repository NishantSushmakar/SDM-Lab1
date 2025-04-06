def execute_query_with_output(session, query, output_file, query_name):
    """Execute a query and write results to the output file"""
    try:
        result = session.run(query)
        output_file.write(f"\n\n--- {query_name} Results ---\n\n")
        
        # Get the column names from the first record
        records = list(result)
        if not records:
            output_file.write("No results returned.\n")
            return
            
        # Write column headers
        keys = records[0].keys()
        header = " | ".join(keys)
        output_file.write(f"{header}\n")
        output_file.write("-" * len(header) + "\n")
        
        # Write data rows
        for record in records:
            row_values = [str(record[key]) for key in keys]
            output_file.write(" | ".join(row_values) + "\n")
            
    except Exception as e:
        output_file.write(f"Error executing {query_name}: {str(e)}\n")
