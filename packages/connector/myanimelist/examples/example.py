import asyncio, os, cognee
from cognee_community_connector_myanimelist import myanimelist_source
async def main():
    if not os.environ.get("MAL_USERNAME"):
        print("Set MAL_USERNAME"); return
    await cognee.remember(myanimelist_source(), dataset_name="myanimelist")
    ans = await cognee.search(query_text="What anime am I watching?", query_type=cognee.SearchType.GRAPH_COMPLETION, datasets=["myanimelist"])
    print(ans)
if __name__ == "__main__": asyncio.run(main())
