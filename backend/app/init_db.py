from sqlalchemy import inspect, text
from .database import Base, engine, SessionLocal

def create_or_replace_elo_records_function():
    drop_function_sql = """
    DROP FUNCTION IF EXISTS get_elo_records_by_fighter(TEXT, TEXT);
    """

    function_sql = """
    CREATE OR REPLACE FUNCTION get_elo_records_by_fighter(
        fighter_name_arg TEXT,
        sort_order_arg TEXT
    )
    RETURNS TABLE (
        id INT,
        fighter_id INT,
        elo_rating FLOAT,
        fighter_name TEXT,
        event_name TEXT,
        event_date DATE,
        total_fights BIGINT
    ) AS $$
    BEGIN
        IF sort_order_arg NOT ILIKE 'asc' AND sort_order_arg NOT ILIKE 'desc' THEN
            RAISE EXCEPTION 'Invalid code: 0015';
        END IF;

        RETURN QUERY EXECUTE
            format(
                'WITH fighter_counts AS (
                    SELECT
                        f.id as fid,
                        COUNT(*) -1 as total_fights
                    FROM elo_records er
                    JOIN fighters f ON er.fighter_id = f.id
                    WHERE f.fighter_name ILIKE ''%%'' || %L || ''%%''
                    GROUP BY f.id
                ),
                ranked_elo AS (
                    SELECT
                        er.id,
                        er.fighter_id,
                        er.elo_rating,
                        f.fighter_name::TEXT,
                        e.event_name::TEXT,
                        e.event_date,
                        fc.total_fights,
                        ROW_NUMBER() OVER (PARTITION BY er.fighter_id ORDER BY e.event_date ASC NULLS FIRST, er.id ASC) as fight_order
                    FROM elo_records er
                    JOIN fighters f ON er.fighter_id = f.id
                    LEFT JOIN events e ON er.event_id = e.id
                    JOIN fighter_counts fc ON fc.fid = f.id
                    WHERE f.fighter_name ILIKE ''%%'' || %L || ''%%''
                )
                SELECT id, fighter_id, elo_rating, fighter_name, event_name, event_date, total_fights
                FROM ranked_elo
                ORDER BY fighter_id, fight_order, event_date %s',
                fighter_name_arg, fighter_name_arg, sort_order_arg
            );
    END;
    $$ LANGUAGE plpgsql;
    """
    session = SessionLocal()  # Make sure SessionLocal is defined in your database.py
    try:
        session.execute(text(drop_function_sql))
        session.commit()

        session.execute(text(function_sql))  # Execute raw SQL within session
        session.commit()  # Commit the transaction
        print("Function created or replaced successfully")
    except Exception as e:
        session.rollback()  # Rollback in case of error
        print(f"Error creating function: {e}")
    finally:
        session.close()  # Close the session
    
def init_db():
    Base.metadata.create_all(bind=engine)
    create_or_replace_elo_records_function()
    inspector = inspect(engine)
    print("Tables in database:", inspector.get_table_names())

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

if __name__ == "__main__":
    init_db()

