import sqlite3
import json


def update_hauler(id, hauler_data):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            UPDATE Hauler
                SET
                    name = ?,
                    dock_id = ?
            WHERE id = ?
            """,
            (hauler_data["name"], hauler_data["dock_id"], id),
        )

        rows_affected = db_cursor.rowcount

    return True if rows_affected > 0 else False


def post_hauler(request_body):
    with sqlite3.connect("./shipping.db") as conn:
        db_cursor = conn.cursor()

        db_cursor.execute(
            """
            INSERT INTO 'Hauler' 
            VALUES (null, ?, ?)
            """,
            (request_body["name"], request_body["dock_id"]),
        )
        hauler = db_cursor.fetchone()
        serialized_hauler = json.dumps(hauler)
    return serialized_hauler


def delete_hauler(pk):
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        DELETE FROM Hauler WHERE id = ?
        """,
            (pk,),
        )
        number_of_rows_deleted = db_cursor.rowcount

    return True if number_of_rows_deleted > 0 else False


def list_haulers(url):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        if "_embed" in url:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id,
                s.id ship_id,
                s.name ship_name,
                s.hauler_id ship_haulerId
            FROM Hauler h
            JOIN Ship s
            ON s.hauler_id = h.id
            ORDER BY h.id
            """
            )
            query_results = db_cursor.fetchall()
            haulers = {}
            for row in query_results:
                hauler_id = row["id"]

                if hauler_id not in haulers:
                    haulers[hauler_id] = {
                        "id": row["id"],
                        "name": row["name"],
                        "dock_id": row["dock_id"],
                        "ships": [],
                    }
                ship = {
                    "id": row["ship_id"],
                    "name": row["ship_name"],
                    "hauler_id": row["ship_haulerId"],
                }
                haulers[hauler_id]["ships"].append(ship)
                serialized_haulers = json.dumps(list(haulers.values()))

        # Write the SQL query to get the information you want
        else:
            db_cursor.execute(
                """
            SELECT
                h.id,
                h.name,
                h.dock_id
            FROM Hauler h
            """
            )
            query_results = db_cursor.fetchall()

            # Initialize an empty list and then add each dictionary to it
            haulers = []
            for row in query_results:
                haulers.append(dict(row))

            # Serialize Python list to JSON encoded string
            serialized_haulers = json.dumps(haulers)

    return serialized_haulers


def retrieve_hauler(pk):
    # Open a connection to the database
    with sqlite3.connect("./shipping.db") as conn:
        conn.row_factory = sqlite3.Row
        db_cursor = conn.cursor()

        # Write the SQL query to get the information you want
        db_cursor.execute(
            """
        SELECT
            h.id,
            h.name,
            h.dock_id
        FROM Hauler h
        WHERE h.id = ?
        """,
            (pk,),
        )
        query_results = db_cursor.fetchone()

        # Serialize Python list to JSON encoded string
        serialized_hauler = json.dumps(dict(query_results))

    return serialized_hauler
