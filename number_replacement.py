from aiogram import types
from config import CHAT_ID
from create_bot import bot
from aiogram import Dispatcher
from sqlite_db import sql_add_data, sql_add_comment
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
import pytz
from datetime import datetime
from keyboards.question_kb import question_take_to_work_kb


class FSMNumberReplacement(StatesGroup):
    user_id = State()
    number_replacement_state = State()


text_message = 'Для отправки сообщения необходимо:\n' \
               'Скопировать номер заявки или номер лицевого счёта из ХХХХХ или ХХХХ, вставить в ' \
               'Telegram и через пробел ввести ваш запрос.\n\n' \
               '1568253-02-01 Запрос\n' \
               'CRM-123456789 Запрос\n' \
               'НЛС: 123456789012 Запрос'


async def call_number_replacement(call: types.CallbackQuery, state: FSMContext):

    await call.answer()

    await state.update_data(user_id=call.from_user.id)

    await bot.send_message(
        chat_id=call.message.chat.id,
        text=f'{call.data}\n\n{text_message}')

    await FSMNumberReplacement.number_replacement_state.set()


async def number_replacement_handler(message: types.Message, state: FSMContext):

    if any((message.text.startswith('CRM'), message.text[0:3].isdigit())) \
            and len(message.text.split(' ')) >= 2 \
            and len(message.text.split(' ')[0]) == 13 or len(message.text.split(' ')[0]) == 15:

        moscow_time = datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S")

        data_main = {
            'date_time': moscow_time,
            'application_number': message.text.split(' ')[0],
            'theme': 'Замена номера УЗ IPTV и OTT',
            'sub_theme': 0,
            'engineer_id': message.from_user.id,
            'question': ' '.join(message.text.split(' ')[1:]),
            'executor_id': 0,
            'message_id': message.message_id,
            'message_chat_id': 0,
            'message_comments_id': 0,
            'active_chat': 1,
            'last_comment': 0,
            'last_date': 0,
            'estimation': 0
        }

        data_engineer = {
            'engineer_id': message.from_user.id,
            'engineer_name': message.from_user.full_name,
            'engineer_url': message.from_user.url
        }

        await sql_add_data(
            data_main=data_main,
            data_engineer=data_engineer)

        ready_text = f"{data_engineer['engineer_name']}\n" \
                     f"{data_main['theme']}\n " \
                     f"{data_main['application_number']}\n" \
                     f"{data_main['question']}\n"

        message_chat_id = await bot.send_message(
            chat_id=CHAT_ID,
            text=f'{ready_text}')

        callback_text = f'work+{data_engineer["engineer_url"]}' \
                        f'+{data_main["message_id"]}' \
                        f'+{data_main["engineer_id"]}' \
                        f'+{message_chat_id.message_id}'

        await bot.send_message(
            chat_id=CHAT_ID,
            text='Прошу взять в работу',
            disable_notification=True,
            reply_markup=question_take_to_work_kb(callback_text)
        )

        await sql_add_comment((message_chat_id['message_id'], data_main['message_id']))

        await message.reply(text='Обращение передано ведущим инженерам\n\n')

        await state.finish()

    elif message.text.startswith('НЛС'):
        if message.text.split(' ')[1].isdigit():
            moscow_time = datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S")

            data_main = {
                'date_time': moscow_time,
                'application_number': message.text.split(' ')[0],
                'theme': 'Замена номера УЗ IPTV и OTT',
                'sub_theme': 0,
                'engineer_id': message.from_user.id,
                'question': ' '.join(message.text.split(' ')[1:]),
                'executor_id': 0,
                'message_id': message.message_id,
                'message_chat_id': 0,
                'message_comments_id': 0,
                'active_chat': 1,
                'last_comment': 0,
                'last_date': 0,
                'estimation': 0
            }

            data_engineer = {
                'engineer_id': message.from_user.id,
                'engineer_name': message.from_user.full_name,
                'engineer_url': message.from_user.url
            }

            await sql_add_data(
                data_main=data_main,
                data_engineer=data_engineer)

            ready_text = f"{data_engineer['engineer_name']}\n" \
                         f"{data_main['theme']}\n " \
                         f"{data_main['application_number']}\n" \
                         f"{data_main['question']}\n"

            message_chat_id = await bot.send_message(
                chat_id=CHAT_ID,
                text=f'{ready_text}')

            callback_text = f'work+{data_engineer["engineer_url"]}' \
                            f'+{data_main["message_id"]}' \
                            f'+{data_main["engineer_id"]}' \
                            f'+{message_chat_id.message_id}'

            await bot.send_message(
                chat_id=CHAT_ID,
                text='Прошу взять в работу',
                disable_notification=True,
                reply_markup=question_take_to_work_kb(callback_text)
            )

            await sql_add_comment((message_chat_id['message_id'], data_main['message_id']))

            await message.reply(text='Обращение передано ведущим инженерам\n\n')

            await state.finish()

    else:

        await message.reply(text_message)


def register_handlers_number_replacement(dp: Dispatcher):
    dp.register_callback_query_handler(call_number_replacement, text='Замена номера УЗ IPTV и OTT')
    dp.register_message_handler(number_replacement_handler, state=FSMNumberReplacement.number_replacement_state)
