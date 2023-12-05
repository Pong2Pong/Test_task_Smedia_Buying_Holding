import sqlalchemy
import asyncpg
import loguru
import pyrogram
from pyrogram import Client
from pyrogram import *
import asyncio
from config import *
import time
from datetime import datetime, timedelta, timezone

client = Client(name=username, api_hash=api_hash, api_id=api_id)


@client.on_message()
async def my_handler(client, message):
    conn = await asyncpg.connect(user='postgres', password='123',
                                 database='Test_task_bd', host='127.0.0.1')
    row = await conn.fetchrow(
        f'SELECT "User_ID" FROM public."Test_task_Smedia_Buying_Holding" WHERE "User_ID" = {message.from_user.id}')
    if row == None:
        local_time = datetime.fromtimestamp(time.time())
        await conn.execute(
            f'''INSERT INTO public."Test_task_Smedia_Buying_Holding"("User_ID", "First_message_ts") VALUES({message.from_user.id}, '{local_time}')''')
    await conn.close()


async def main():
    found_good_day_flag = False
    await client.start()
    me = await client.get_me()
    conn = await asyncpg.connect(user='postgres', password='123',
                                 database='Test_task_bd', host='127.0.0.1')
    while True:
        rows = await conn.fetch(
            f'SELECT * FROM public."Test_task_Smedia_Buying_Holding" ORDER BY "First_message_ts"')

        for row in rows: #Вынести в отдельную функцию
            if row ["First_message_ts"] + timedelta(minutes=10) < datetime.now() and \
                datetime.now() < row ["First_message_ts"] + timedelta(minutes=10, seconds=10):
                await client.send_message(row ["User_ID"], "Добрый день!")
            if row ["First_message_ts"] + timedelta(minutes=90) < datetime.now() and \
                datetime.now() < row ["First_message_ts"] + timedelta(minutes=90, seconds=10):
                await client.send_message(row ["User_ID"], "Подготовила для вас материал")
                await client.send_photo(row ["User_ID"], "img.png")
            if row ["First_message_ts"] + timedelta(minutes=120) < datetime.now() and \
                datetime.now() < row ["First_message_ts"] + timedelta(minutes=120, seconds=10):
                async for message in client.get_chat_history(row ["User_ID"]):
                    if message.text == "Хорошего дня" and message.from_user.id == me.id:
                        print(message)
                        found_good_day_flag = True
                        break
                if not found_good_day_flag:
                    await client.send_message(row ["User_ID"], "Скоро вернусь с новым материалом!")
        await asyncio.sleep(9.5)




ioloop = asyncio.get_event_loop()
tasks = [
    ioloop.create_task(main())
]
ioloop.run_until_complete(asyncio.wait(tasks))
ioloop.close()
client.run()


