from aiohttp import web
from sqlalchemy import Column, Integer, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import json


PG_DSN = 'postgresql+asyncpg://user:password@127.0.0.1:5432'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()


class Advert(Base):

    __tablename__ = 'advertisements'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=False)
    owner = Column(String, nullable=False)
    creation_date = Column(DateTime, server_default=func.now())


app = web.Application()


async def app_context(app):
    print('Start')
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    print('Exit')


@web.middleware
async def session_middleware(request: web.Request, handler):
    async with Session() as session:
        request['session'] = session
        response = await handler(request)
        return response


app.cleanup_ctx.append(app_context)
app.middlewares.append(session_middleware)


async def get_advert(advert_id: int, session: Session):
    advert = await session.get(Advert, advert_id)
    if advert is None:
        raise web.HTTPNotFound(text=json.dumps({'status': 'error', 'message': 'advert not found'}),
                            content_type='application/json')
    return advert


class AdvertView(web.View):
    async def get(self):
        advert = await get_advert(int(self.request.match_info['advert_id']), self.request['session'])
        return web.json_response({
            'id': advert.id,
            'title': advert.title,
            "description": advert.description,
            "creation_date": advert.creation_date.strftime("%d.%m.%y"),
            "owner": advert.owner
        })


    async def post(self):
        json_data = await self.request.json()
        advert = Advert(**json_data)
        self.request['session'].add(advert)
        try:
            await self.request['session'].commit()
        except IntegrityError as er:
            raise web.HTTPConflict(
                text=json.dumps({'status': 'error', 'message': 'there is already an ad with this name'}),
                content_type='application/json'
            )
        return web.json_response({'status': 'success', 'message': 'advert has been posted',
                                'id': advert.id, 'title': advert.title})


    async def delete(self):
        advert = await get_advert(int(self.request.match_info['advert_id']), self.request['session'])
        await self.request['session'].delete(advert)
        await self.request['session'].commit()
        return web.json_response({'status': 'success', 'message': 'advert has been deleted'})


app.add_routes([
    web.get('/adverts/{advert_id:\d+}/', AdvertView),
    web.post('/adverts/', AdvertView),
    web.delete('/adverts/{advert_id:\d+}/', AdvertView),
])


if __name__ == '__main__':
    web.run_app(app)