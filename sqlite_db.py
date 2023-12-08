import sqlite3 as sq


def sql_start():
    global base, cur
    base = sq.connect('data.db')
    cur = base.cursor()
    if base:
        print('Data base connected!')

    base.execute(
        'CREATE TABLE IF NOT EXISTS main '
        '(date_time TEXT, '
        'application_number TEXT, '
        'theme TEXT, '
        'sub_theme TEXT, '
        'engineer_id INTEGER, '
        'question TEXT, '
        'executor_id INTEGER, '
        'message_id INTEGER, '
        'message_chat_id INTEGER, '
        'message_comments_id INTEGER, '
        'active_chat INTEGER, '
        'last_comment INTEGER, '
        'last_date TEXT, '
        'estimation TEXT)')
    base.commit()

    base.execute(
        'CREATE TABLE IF NOT EXISTS engineers '
        '(engineer_id INTEGER UNIQUE, '
        'engineer_name TEXT, '
        'engineer_url TEXT)')
    base.commit()

    base.execute(
        'CREATE TABLE IF NOT EXISTS access '
        '(engineer_id INTEGER UNIQUE, '
        'access_level INTEGER, '
        'invited_id INTEGER, '
        'date_added	TEXT)')
    base.commit()

    base.execute(
        'CREATE TABLE IF NOT EXISTS access_names '
        '(level	INTEGER, '
        'name TEXT)')
    base.commit()


async def sql_add_data(data_main, data_engineer):
    cur.execute('INSERT INTO main('
                'date_time, application_number, theme, sub_theme, engineer_id, question, executor_id, message_id, '
                'message_chat_id, message_comments_id, active_chat, last_comment, last_date, estimation) '
                'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(data_main.values()))

    cur.execute('INSERT OR IGNORE INTO engineers(engineer_id, engineer_name, engineer_url) '
                'VALUES(?, ?, ?)', tuple(data_engineer.values()))

    base.commit()


async def sql_add_data_no_help(data_main):
    cur.execute('INSERT INTO main('
                'date_time, application_number, theme, sub_theme, engineer_id, question, executor_id, message_id, '
                'message_chat_id, message_comments_id, active_chat, last_comment, last_date, estimation) '
                'VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(data_main.values()))

    base.commit()


async def sql_add_implementer(data, data_executor):
    cur.execute('UPDATE main SET executor_id == ? WHERE message_id == ?', tuple(data))

    cur.execute('INSERT OR IGNORE INTO engineers(engineer_id, engineer_name, engineer_url) '
                'VALUES(?, ?, ?)', tuple(data_executor.values()))

    base.commit()


async def sql_add_feedback(data_main, data_engineer):
    cur.execute('INSERT INTO main'
                '(date_time, application_number, theme, sub_theme, engineer_id, question, executor_id, message_id) '
                'VALUES(?, ?, ?, ?, ?, ?, ?, ?)', tuple(data_main.values()))

    cur.execute('INSERT OR IGNORE INTO engineers(engineer_id, engineer_name, engineer_url) '
                'VALUES(?, ?, ?)', tuple(data_engineer.values()))

    base.commit()


async def sql_get_history():
    msg = set()
    for el in cur.execute('SELECT engineer_id FROM access').fetchall():
        msg.add(int(el[0]))
    return msg


async def sql_add_access(data):
    cur.execute('INSERT OR REPLACE INTO access (engineer_id, access_level, invited_id, date_added) '
                'VALUES (?, ?, ?, ?)', tuple(data))
    base.commit()


async def sql_delete_access(data):
    cur.execute(f'DELETE FROM access WHERE engineer_id={data}')
    base.commit()


async def sql_get_access(data):
    result = cur.execute(f'SELECT access_level FROM access WHERE engineer_id={data}').fetchone()
    return result


async def sql_get_report_questions(start_date, end_dat):
    request = f"""
    SELECT date_time as Дата, 
    application_number as Номер_заявки, 
    theme as Тематика, 
    sub_theme as Подтема, 
    a.engineer_name as Инженер, 
    question as Вопрос, 
    c.engineer_name as Исполнитель,
    d.engineer_name as Супервайзер,
    estimation as Требовалась_помощь
    FROM main 
    JOIN engineers a ON a.engineer_id = main.engineer_id 
    JOIN engineers c ON c.engineer_id = main.executor_id
    JOIN access ac ON ac.engineer_id = main.engineer_id
    JOIN engineers d ON ac.invited_id = d.engineer_id
    WHERE theme != "Опен ВПН" 
    AND theme != "Предложение"
    AND theme != "Доступ на СПД" AND main.date_time BETWEEN "{start_date}" AND "{end_dat}"
    ORDER BY Инженер, Дата
    """
    result = [('Дата', 'Номер_заявки', 'Тематика', 'Подтема', 'Инженер', 'Вопрос', 'Исполнитель', 'Супервайзер',
               'Требовалась_помощь')]
    for el in cur.execute(request).fetchall():
        result.append(el)

    return result


async def sql_get_report_gp(start_date, end_dat):
    request = f"""
    SELECT date_time as Дата, 
    application_number as Номер_заявки, 
    theme as Тематика, 
    sub_theme as Подтема, 
    a.engineer_name as Инженер, 
    question as Вопрос, 
    c.engineer_name as Исполнитель,
    d.engineer_name as Супервайзер,
    estimation as Требовалась_помощь
    FROM main 
    JOIN engineers a ON a.engineer_id = main.engineer_id 
    JOIN engineers c ON c.engineer_id = main.executor_id
    JOIN access ac ON ac.engineer_id = main.engineer_id
    JOIN engineers d ON ac.invited_id = d.engineer_id
    WHERE main.theme = "Проверка на ГП" AND main.date_time BETWEEN "{start_date}" AND "{end_dat}"
    ORDER BY Инженер, Дата
    """
    result = [('Дата', 'Номер_заявки', 'Тематика', 'Подтема', 'Инженер', 'Вопрос', 'Исполнитель', 'Суперайзер',
               'Требовалась_помощь')]
    for el in cur.execute(request).fetchall():
        result.append(el)

    return result


async def sql_get_report_feedback(start_date, end_dat):
    request = f"""
        SELECT date_time Дата, a.engineer_name Имя, question Сообщение 
        FROM main 
        JOIN engineers a ON a.engineer_id = main.engineer_id 
        WHERE theme == "Предложение" AND date_time BETWEEN "{start_date}" AND "{end_dat}"
        ORDER BY Имя, Дата
    """
    result = [('Дата', 'Имя', 'Сообщение')]

    for el in cur.execute(request).fetchall():
        result.append(el)

    return result


async def sql_get_report_vpn_spd(start_date, end_dat):
    request = f"""
    SELECT date_time as Дата, 
    application_number as Номер_заявки, 
    theme as Тематика, 
    a.engineer_name as Инженер, 
    question as Вопрос
    FROM main 
    JOIN engineers a ON a.engineer_id = main.engineer_id 
    WHERE (main.theme = "Доступ на СПД" OR main.theme = "Опен ВПН") AND main.date_time BETWEEN "{start_date}" AND "{end_dat}"
    ORDER BY Инженер, Дата
    """
    result = [('Дата', 'Номер_заявки', 'Тематика', 'Инженер', 'Вопрос')]
    for el in cur.execute(request).fetchall():
        result.append(el)

    return result


async def sql_add_comment(data_main):
    cur.execute('UPDATE main SET message_chat_id == ? WHERE message_id == ?', tuple(data_main))
    base.commit()


async def sql_close_chat(data_main):
    cur.execute(f'UPDATE main SET active_chat == 0 WHERE engineer_id == {data_main} AND active_chat == 1')
    base.commit()


async def sql_add_id_comment(data):
    cur.execute('UPDATE main SET message_comments_id == ? WHERE message_chat_id == ?', tuple(data))

    base.commit()


async def sql_check_id_chat(data):
    request = f'SELECT * FROM main WHERE message_chat_id == {data}'

    result = cur.execute(request).fetchone()

    return result


async def sql_check_id_chat1(data):
    request = f'SELECT * FROM main WHERE message_comments_id == {data}'

    result = cur.execute(request).fetchone()

    return result


async def sql_check_id_comment(data):
    request = f'SELECT * FROM comments WHERE message_comments_id == {data}'

    result = cur.execute(request).fetchone()

    return result


async def sql_get_comment(data):
    request = f'SELECT * FROM main WHERE engineer_id == {data} AND active_chat == 1'

    result = cur.execute(request).fetchone()
    return result


async def sql_get_question(message_id):

    request = f'SELECT a.engineer_name, application_number, theme, sub_theme, question FROM main ' \
              f'JOIN engineers a ON a.engineer_id = main.engineer_id ' \
              f'WHERE message_id == {message_id}'

    result = cur.execute(request).fetchone()
    return result


async def sql_set_last_comment_date(data):
    cur.execute(f'UPDATE main SET last_comment == ?, last_date == ? WHERE message_chat_id == ?', tuple(data))
    base.commit()


async def sql_get_application_data(message_comments_id):
    request = 'SELECT message_chat_id, a.engineer_name, theme, sub_theme, ' \
              'application_number, question, b.engineer_name FROM main ' \
              'JOIN engineers a ON a.engineer_id = main.engineer_id ' \
              'JOIN engineers b ON b.engineer_id = main.executor_id ' \
              f'WHERE message_comments_id == {message_comments_id}'

    result = cur.execute(request).fetchone()
    return result


async def sql_get_id(data):
    request = f'SELECT message_comments_id FROM main WHERE message_chat_id == {data}'

    result = cur.execute(request).fetchone()[0]
    return result


async def sql_add_estimation(data_main):
    cur.execute('UPDATE main SET estimation == ? WHERE message_chat_id == ?', tuple(data_main))
    base.commit()


async def sql_get_date_added(data):
    result = cur.execute(f'SELECT date_added FROM access WHERE engineer_id={data}').fetchone()
    return result


async def sql_get_report_for_day(date_search):
    resp = f"""
    SELECT
    SUM(CASE WHEN "theme" = 'ШПД / Интернет' THEN 1 ELSE 0 END) AS "ШПД / Интернет",
    SUM(CASE WHEN "theme" = 'ШПД / Интернет' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / ШПД / Интернет",
    SUM(CASE WHEN "theme" = 'Телевидение' THEN 1 ELSE 0 END) AS "Телевидение",
	SUM(CASE WHEN "theme" = 'Телевидение' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Телевидение",
    SUM(CASE WHEN "theme" = 'Домофон / Видеонаблюдение' THEN 1 ELSE 0 END) AS "Домофон / Видеонаблюдение",
    SUM(CASE WHEN "theme" = 'Домофон / Видеонаблюдение' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Домофон / Видеонаблюдение",
    SUM(CASE WHEN "theme" = 'Тайм-Слоты / Домовая книга' THEN 1 ELSE 0 END) AS "Тайм-Слоты / Домовая книга",
    SUM(CASE WHEN "theme" = 'Тайм-Слоты / Домовая книга' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Тайм-Слоты / Домовая книга",
    SUM(CASE WHEN "theme" = 'Самовосстановление' THEN 1 ELSE 0 END) AS "Самовосстановление",
    SUM(CASE WHEN "theme" = 'Самовосстановление' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Самовосстановление",
    SUM(CASE WHEN "theme" = 'Диагностика оборудования СПД' THEN 1 ELSE 0 END) AS "Диагностика оборудования СПД",
    SUM(CASE WHEN "theme" = 'Диагностика оборудования СПД' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Диагностика оборудования СПД",
    SUM(CASE WHEN "theme" = 'Замена номера УЗ IPTV и OTT' THEN 1 ELSE 0 END) AS "Замена номера УЗ IPTV и OTT",
    SUM(CASE WHEN "theme" = 'Замена номера УЗ IPTV и OTT' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Замена номера УЗ IPTV и OTT",
    SUM(CASE WHEN "theme" = 'Взаимодействие с другими отделами' THEN 1 ELSE 0 END) AS "Взаимодействие с другими отделами",
    SUM(CASE WHEN "theme" = 'Взаимодействие с другими отделами' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Взаимодействие с другими отделами",
    SUM(CASE WHEN "theme" = 'Речевые модули' THEN 1 ELSE 0 END) AS "Речевые модули",
    SUM(CASE WHEN "theme" = 'Речевые модули' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Речевые модули",
    SUM(CASE WHEN "theme" = 'Инструкции по настройке СРЕ' THEN 1 ELSE 0 END) AS "Инструкции по настройке СРЕ",
    SUM(CASE WHEN "theme" = 'Инструкции по настройке СРЕ' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Инструкции по настройке СРЕ",
    SUM(CASE WHEN "theme" = 'Назначить / снять / согласовать заявку' THEN 1 ELSE 0 END) AS "Назначить / снять / согласовать заявку",
    SUM(CASE WHEN "theme" = 'Назначить / снять / согласовать заявку' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Назначить / снять / согласовать заявку",
    SUM(CASE WHEN "theme" = 'Проверка на ГП' THEN 1 ELSE 0 END) AS "Проверка на ГП",
	SUM(CASE WHEN "theme" = 'Проверка на ГП' AND estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Проверка на ГП",
 	SUM(CASE WHEN estimation = 'Да' THEN 1 ELSE 0 END) AS "Полезность / Да", 
	SUM(CASE WHEN estimation = 'Нет' THEN 1 ELSE 0 END) AS "Полезность / Нет",
 	SUM(CASE WHEN estimation = 0 THEN 1 ELSE 0 END) AS "Полезность / Без ответа"
    FROM main WHERE date_time LIKE "{date_search}%";
    """

    res_values = cur.execute(resp).fetchall()[0]
    res_columns = [column[0] for column in cur.description]

    return res_values, res_columns
