import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pytz
from datetime import datetime


tag_dict = {}


async def fetch_data_with_tags(session, tag):
    fetch_url = f"https://XXXXXXXXXXXXXXX/?s=%23{tag}"  # URL-адрес для получения данных

    try:
        if tag not in tag_dict:
            async with session.get(fetch_url) as response:
                data = await response.text()
                soup = BeautifulSoup(data, "html.parser")
                div_tag_search = soup.find(name='div', class_='post')
                a_tag = div_tag_search.find('a')
                link_search = a_tag['href']

            async with session.get(link_search) as f_response:
                f_data = await f_response.text()
                f_soup = BeautifulSoup(f_data, "html.parser")
                f_div_tag_search = f_soup.find(name='div', class_='entry-content')
                f_a_tag = f_div_tag_search.find('a')
                f_link_search = f_a_tag['href']
                f_description = f_a_tag.text.strip()
                result = f'<a href="{f_link_search}">{f_description}</a>'
                tag_dict[tag] = result
            return result

        else:
            return tag_dict[tag]

    except Exception as e:
        moscow_time = datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S")

        with open('errors_parser.txt', 'a') as file:
            file.write(f'{tag}//{e} {moscow_time}\n')

            return f'ССЫЛКА НЕ ДОСТУПНА. <a href="https://t.me/SrgSemenov">Сообщите об ошибке</a>'


async def get_links_for_tags(*tags):
    login_url = "https://XXXXXXXXXXXXXXXXXXX.php"  # URL-адрес для авторизации
    login_data = {'log': 'XXXXXX', 'pwd': 'XXXXXX'}  # Данные для авторизации

    async with aiohttp.ClientSession() as session:
        # Авторизация
        await session.post(login_url, data=login_data)
        tasks = []

        for tag in tags:
            task = asyncio.ensure_future(fetch_data_with_tags(session, tag))
            tasks.append(task)

        results = await asyncio.gather(*tasks)
        res = '\n'.join(results)
        return res
