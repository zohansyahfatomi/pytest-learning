from app.sql import Databases

db = Databases()

def test_select_db():
    result = db.select_db(1)
    #print(result.get('mangga'))
    assert result.get("kata") == "mangga"

def test_insert_db():
    assert db.insert_db("jeruk")


def test_update_Db():
    assert db.update_db("rambutan","jeruk")

