from config import PATH_DB
import sqlite3 

def get_db_connection():
    return sqlite3.connect(PATH_DB)

def add_qid_to_db(id_article, qid, qid_description):
    """
    Update the qid column in the destination table with the given qid for the specified otry_id
    """
    query = """
            UPDATE destination
            SET qid = ?, qidDescription = ?
            WHERE id = ? 
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (qid, qid_description, id_article))
        conn.commit()

def read_top_100_rows():
    """
    Read and return the top 100 rows from the destination table
    """
    query = """
            SELECT title, subject, qid, qidDescription
            FROM destination
            LIMIT 100;
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

def read_top_100_rows_with_qid():
    """
    Read and return the top 100 rows from the destination table where qid is not None, null, or empty
    """
    query = """
            SELECT title, subject, qid, qidDescription
            FROM destination
            WHERE qid IS NOT NULL AND qid != ''
            LIMIT 100;
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

import pandas as pd
import sqlite3

def export_sqlite_to_excel(data, excel_filename):
    """
    Export SQLite data to an Excel file.
    
    :param data: List of tuples containing the data to export
    :param excel_filename: Name of the Excel file to create
    """
    # Create a DataFrame from the data
    df = pd.DataFrame(data, columns=["title", "subject", "qid", "qidDescription"])
    
    # Write DataFrame to Excel
    df.to_excel(excel_filename, index=False)

def read_top_100_rows_with_qid():
    """
    Read and return the top 100 rows from the destination table where qid is not None, null, or empty
    """
    query = """
            SELECT title, subject, qid, qidDescription
            FROM destination
            WHERE qid IS NOT NULL AND qid != ''
            LIMIT 100;
            """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows

# Assuming you have a function get_db_connection() that returns a database connection
# You can call the read_top_100_rows_with_qid() function to get the data and then export it to Excel


# Example usage
if __name__ == "__main__":
    data = read_top_100_rows_with_qid()
    export_sqlite_to_excel(data, "output.xlsx")
