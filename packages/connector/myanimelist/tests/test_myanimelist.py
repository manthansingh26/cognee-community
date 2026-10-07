"""Tests — 5 unit + 1 pipeline (no live API)."""
import pytest
import dlt
from cognee_community_connector_myanimelist.myanimelist import _anime_to_row

def test_row_exact_shape_and_stable_id():
    anime={"mal_id":42,"title":"T","url":"https://m.a/42","synopsis":"s"}
    row=_anime_to_row(anime,{"status":"watching","score":8,"num_episodes_watched":3})
    assert row=={"id":"anime:42","url":"https://m.a/42","title":"T","content":row["content"]}
    assert set(row.keys())=={"id","url","title","content"}

def test_content_markdown():
    anime={"mal_id":1,"title":"A","synopsis":"syn","genres":[{"name":"Act"}],"studios":[{"name":"S1"}],"score":7.5,"url":"https://m.a/1"}
    r=_anime_to_row(anime,{"status":"completed","score":9,"num_episodes_watched":12})
    assert "## A" in r["content"]
    assert "Status: completed" in r["content"]
    assert "MAL score: 7.5" in r["content"]

def test_stable_id_on_edit():
    assert _anime_to_row({"mal_id":7,"title":"X","url":"u"},{"status":"watching","score":0,"num_episodes_watched":0})["id"]=="anime:7"

def test_defensive_no_synopsis():
    r=_anime_to_row({"mal_id":99,"title":"Y","url":"u"},{"status":"plan_to_watch","score":0,"num_episodes_watched":0})
    assert "### Synopsis" not in r["content"]

def test_defensive_no_genres():
    r=_anime_to_row({"mal_id":55,"title":"Z","url":"u"},{"status":"dropped","score":3,"num_episodes_watched":2})
    assert "Genres:" not in r["content"]

def test_pipeline_sync_edit_forget(tmp_path, monkeypatch):
    import dlt
    from cognee_community_connector_myanimelist.myanimelist import myanimelist_source
    class FakeResp:
        def __init__(self,d): self.status_code=200; self.headers={"content-type":"application/json","link":""}; self._d=d
        def json(self): return self._d
        def text(self): return str(self._d)
        def raise_for_status(self):
            if self.status_code>=400: raise Exception(str(self.status_code))
    class FakeC:
        def __init__(self,d): self.base_url="https://api.jikan.moe/v4"; self._d=d
        def get(self,url,timeout=30.0,**kw):
            p=url.replace(self.base_url,"")
            return FakeResp(self._d.get(p,{"data":[],"pagination":{"has_next_page":False}}))
    data={
        "/users/test/animelist?limit=25&page=1":{"data":[{"anime":{"mal_id":1,"title":"A","url":"https://m.a/1"},"list_status":{"status":"watching","score":7,"num_episodes_watched":3}}],"pagination":{"has_next_page":False}},
        "/anime/1/full":{"data":{"mal_id":1,"title":"A","synopsis":"syn","genres":[{"name":"G1"}],"studios":[{"name":"S1"}],"score":7.5,"url":"https://m.a/1"}},
    }
    fake=FakeC(data)
    monkeypatch.setattr("httpx.Client",lambda **kw:fake)
    db_path=(tmp_path/"mal.db").as_posix()
    pl=dlt.pipeline(pipeline_name="mal",destination=dlt.destinations.sqlalchemy(f"sqlite:///{db_path}"),dataset_name="mal",pipelines_dir=str(tmp_path/"state"))
    pl.run(myanimelist_source(username="test"))
    data["/users/test/animelist?limit=25&page=1"]["data"][0]["list_status"]["score"]=9
    pl.run(myanimelist_source(username="test"))
    data["/users/test/animelist?limit=25&page=1"]["data"]=[]
    pl.run(myanimelist_source(username="test"))
    with pl.sql_client() as c:
        with c.execute_query("SELECT id FROM myanimelist_documents") as cur:
            ids=[r[0] for r in cur.fetchall()]
    assert "anime:1" not in ids
