from app.sql import Databases

db = Databases(kata = "kata1")

def test_select_db():
    db.select_db()
    assert db.kata == 10
    
'''
def test_insert_db():
    db.insert_db()
'''