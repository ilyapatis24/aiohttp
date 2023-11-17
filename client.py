from aiohttp import ClientSession
from asyncio import run

async def main():
    async with ClientSession() as session:

        response = await session.post('http://127.0.0.1:8080/adverts/', json={
            "title": "New sportcar",
            "description": "Very expensive and very fast new sportcar from Audi",
            "owner": "Ilya"
        })
        print(await response.json())

        response = await session.get('http://127.0.0.1:8080/adverts/1/')
        print(await response.json())

        response = await session.delete('http://127.0.0.1:8080/adverts/1/')
        print(await response.json())


run(main())