You can quickly explore a DuckDB database primarily using the **Command Line Interface (CLI)** and a set of **Dot Commands** or **standard SQL** queries.

Here are the essential steps and commands for rapid database exploration:

-----

## 🚀 Getting Started with the CLI

1.  **Launch the CLI:**
    If you have a persistent database file (e.g., `my_data.duckdb`), connect to it. Otherwise, it will open an in-memory database by default.

    ```bash
    $ duckdb my_data.duckdb
    ```

    *The prompt will change to `D` or a similar indicator.*

2.  **Display Databases (Optional):**
    If you've attached multiple databases or want to confirm the current one, use the `.databases` dot command.

    ```sql
    .databases
    ```

-----

## 🔍 Database and Table Discovery

These commands give you a quick overview of what's inside the database.

  * **List all Tables:**
    Use the `.tables` dot command to see all tables (and views) in the current database.

    ```sql
    .tables
    ```

  * **View Table Schema:**
    Use the `.schema` dot command followed by the table name to inspect its column names, data types, and any constraints.

    ```sql
    .schema table_name
    ```

  * **View All Schema (including `CREATE TABLE` statements):**
    For a more detailed view of the entire database structure.

    ```sql
    .schema
    ```

-----

## 📊 Data Inspection

Once you know the table names and schemas, you can inspect the data itself.

  * **Quick Data Peek (Top Rows):**
    Use a standard SQL `SELECT` query with a `LIMIT` clause to see the first few rows and get a feel for the data.

    ```sql
    SELECT * FROM table_name LIMIT 10;
    ```

  * **Quick Dataset Summary:**
    DuckDB has a built-in function to generate a quick summary (min, max, mean, null counts, etc.) for a table, similar to a `describe()` function in a DataFrame library.

    ```sql
    PRAGMA table_info('table_name'); -- Shows column types (similar to .schema)

    SELECT * FROM summarize('table_name'); -- Provides statistical summary
    ```

  * **Count Rows:**
    Find out the size of a table.

    ```sql
    SELECT COUNT(*) FROM table_name;
    ```

-----

## 🔧 Useful CLI "Dot Commands"

Dot commands are client-specific and help with the exploration environment:

| Command | Description |
| :--- | :--- |
| **`.help`** | Displays a list of all dot commands. |
| **`.timer on`** | Turns on a timer to measure query execution time, useful for performance evaluation. |
| **`.mode [MODE]`** | Changes the output format (e.g., `markdown`, `json`, `csv`). Use `.mode` to see the current setting. |
| **`.quit`** | Exits the DuckDB CLI session. |

-----

Would you like to try one of these commands, or are you looking for specific instructions on how to use DuckDB with **Python/Pandas**?