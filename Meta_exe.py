import pandas as pd
import numpy as np
import pyodbc
import csv
import zlib
import sys
import os
import re
import io


# Путь для выходных файлов
csv_path                    = args[0]
db_name                     = args[2]
db_type                     = args[5]

if not os.path.exists(csv_path):
    try:
        os.makedirs(csv_path)  # Создаём директорию (включая все недостающие родительские)
        print(f"Директория {csv_path} успешно создана")
    except OSError as e:
        print(f"Ошибка при создании директории: {e}")
else:
    print(f"Директория {csv_path} уже существует")
    
# Параметры подключения
if db_type == 'PG':
    server_with_port = args[1] if ':' in args[1] else f"{args[1]}:5432"
    conn_str = (
        f"DRIVER={{PostgreSQL Unicode}};"  # Драйвер для PostgreSQL
        f"SERVER={args[1]};"      # Адрес сервера с портом, если не указан
        f"DATABASE={args[2]};"
        f"UID={args[3]};"
        f"PWD={args[4]}"
    )
else:
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={args[1]};"
        f"DATABASE={args[2]};"
        f"UID={args[3]};"
        f"PWD={args[4]}"
)


def parse_DBSchema(cursor):  # Теперь принимает cursor

    def clean_and_prepare_content(raw_data):
        """Очищаем и подготавливаем данные для парсинга"""
        if isinstance(raw_data, bytes):
            if raw_data.startswith(b'\xef\xbb\xbf'):
                raw_data = raw_data[3:]
            content = raw_data.decode("utf-8", errors="replace")
        else:
            content = str(raw_data)

        content = content.replace('""', '"')
        # Это важно: убираем пробелы вокруг ключевых символов для упрощения regex
        content = re.sub(r'\s*,\s*', ',', content)
        content = re.sub(r'\s*{\s*', '{', content)
        content = re.sub(r'\s*}\s*', '}', content)

        print(f"[DEBUG-CLEAN] Первые 500 символов после очистки: {repr(content[:500])}")
        return content

    def parse_schema_data(content):

        def find_block_end(content, start_pos, open_char='{', close_char='}'):
            """Находит конец блока, учитывая вложенные скобки."""
            bracket_count = 1
            pos = start_pos
            while pos < len(content) and bracket_count > 0:
                if content[pos] == open_char:
                    bracket_count += 1
                elif content[pos] == close_char:
                    bracket_count -= 1
                pos += 1
            if bracket_count == 0:
                return pos
            return -1

        def parse_fields_in_block(table_name, fields_block, results, is_table_part=False):
            """Парсит поля в блоке таблицы (или табличной части), обрабатывая одиночные и множественные типы."""

            def parse_all_types_for_field(field_types_block_str, field_name, table_name, is_table_part, results):
                """ Парсит блок, содержащий один или несколько типов для поля, и добавляет записи в results.
                    field_types_block_str: строка вида '{6,{"E",...},{"L",...}}' или '{1,{"N",...}}'
                """

                def parse_single_field_type(type_block_str):
                    """
                    Парсит один блок типа (например, '{"S",2147483648,0,"",0}' или '{"R",0,0,"Enum13653",2}')
                    и возвращает код типа и ReferencedObject.
                    """
                    # Этот паттерн должен захватить код типа (S, N, R и т.д.)
                    # и, если есть, вторую строку в кавычках (которая будет ReferencedObject для R)
                    match = re.search(r'\{"([A-Z])"(?:,[^"]*?"([^"]*)")?', type_block_str)

                    if match:
                        field_type_code = match.group(1)
                        referenced_object = ""

                        # Если тип "R" (ссылка), пытаемся найти ReferencedObject
                        if field_type_code == 'R':
                            # Ищем все строки в кавычках. ReferencedObject - это обычно вторая строка.
                            all_quoted_strings = re.findall(r'"([^"]*)"', type_block_str)
                            if len(all_quoted_strings) > 1:
                                potential_referenced_object = all_quoted_strings[1]
                                # Убедимся, что это не просто пустое значение или число, а реальная ссылка
                                if potential_referenced_object and not re.fullmatch(r'\d+',
                                                                                    potential_referenced_object):
                                    referenced_object = potential_referenced_object

                        return field_type_code, referenced_object

                    print(f"[DEBUG-TYPE-ERROR] Не удалось спарсить одиночный тип из: {repr(type_block_str)}")
                    return None, ""

                # Глобальный маппинг типов DBSchema
                type_mapping = {
                    'S': 'Строка',
                    'N': 'Число',
                    'T': 'Дата',
                    'B': 'ДвоичныеДанные',
                    'V': 'ДвоичныеДанные',
                    'L': 'Булево',
                    'R': 'Ссылка',
                    'E': 'ТипЗначения'
                }
                # Ищем отдельные блоки типов, например: {"S",...}, Мы ищем сбалансированные скобки для каждого типа.
                single_type_pattern = re.compile(r'\{"([A-Z])"[^{}]*(?:\{[^{}]*\}[^{}]*)*?\}', re.DOTALL)

                type_matches = list(single_type_pattern.finditer(field_types_block_str))

                num_types = len(type_matches)  # Определяем количество типов
                print(f"[DEBUG-TYPES] Найдено {num_types} отдельных типов для поля '{field_name}'")

                if not type_matches:
                    print(f"[DEBUG-TYPES] ⚠️ Нет отдельных типов в блоке: {repr(field_types_block_str)} для поля {field_name}. Попытка обработки как одиночного.")
                    # Попытаемся обработать блок как один тип, если не нашли {"X",...}
                    field_type_code, referenced_object = parse_single_field_type(field_types_block_str)
                    if field_type_code:
                        readable_type = type_mapping.get(field_type_code, field_type_code)

                        # Логика формирования DisplayFieldName для fallback случая (нет отдельных типов)
                        # В этом случае, если мы не смогли распарсить отдельные блоки,
                        # но смогли получить один код типа, будем считать это одиночным типом.
                        display_field_name = "_" + field_name  # Просто имя без суффикса, так как это одиночный тип
                        if field_type_code == 'E':  # Если это композитный тип, но он единственный, то добавляем суффикс
                            display_field_name = f"_{field_name}_TYPE"
                        elif num_types > 1:  # Если каким-то образом num_types стало > 1, хотя match не было, добавляем суффикс
                            display_field_name = f"_{field_name}_{field_type_code}"

                        referenced_object = f"_{referenced_object}" if referenced_object else referenced_object
                        results.append({
                            'TableName': table_name,
                            'FieldName': display_field_name,  # FieldName остается базовым для JOIN
                            'FieldType': readable_type,
                            'FieldTypeCode': field_type_code,
                            'ReferencedObject': referenced_object,
                            'IsTablePart': is_table_part
                        })
                        print(
                            f"[DEBUG-TYPES]  ✓ Добавлено (fallback): {field_name} ({display_field_name}, {readable_type}) -> {referenced_object}")
                    return

                for type_match in type_matches:
                    single_type_block_str = type_match.group(0)  # Весь блок одного типа: '{"S",...}'
                    field_type_code, referenced_object = parse_single_field_type(single_type_block_str)

                    if field_type_code:
                        readable_type = type_mapping.get(field_type_code, field_type_code)

                        # --- ИЗМЕНЕННАЯ ЛОГИКА ФОРМИРОВАНИЯ DisplayFieldName ---
                        if num_types > 1:  # Если типов больше одного, добавляем суффикс
                            if field_type_code == 'E':
                                display_field_name = f"_{field_name}_TYPE"
                            else:
                                display_field_name = f"_{field_name}_{field_type_code}"
                        else:  # Если тип только один, не добавляем суффикс
                            display_field_name = "_" + field_name
                        # ----------------------------------------------------

                        referenced_object = f"_{referenced_object}" if referenced_object else referenced_object
                        results.append({
                            'TableName': table_name,
                            'FieldName': display_field_name,  # FieldName остается базовым для JOIN
                            'FieldType': readable_type,
                            'FieldTypeCode': field_type_code,
                            'ReferencedObject': referenced_object,
                            'IsTablePart': is_table_part
                        })
                        print(
                            f"[DEBUG-TYPES]  ✓ Добавлено: {field_name} ({display_field_name}, {readable_type}) -> {referenced_object}")
                    else:
                        print(
                            f"[DEBUG-TYPES]  ❌ Не удалось определить тип из '{single_type_block_str}' для поля '{field_name}'")


            print(f"[DEBUG-PARSE-FIELDS] Парсинг полей для таблицы '{table_name}'. Длина блока: {len(fields_block)}")
            print(f"[DEBUG-PARSE-FIELDS] Начало блока: {repr(fields_block[:200])}")

            # Паттерн для захвата имени поля и ВСЕГО блока его типов.
            # group(1): FieldName (e.g., "Fld123")
            # group(2): Full type block (e.g., '{1,{"N",15,0,"",0}}' or '{6,{"E",...},{"R",...}}')
            field_pattern = re.compile( r'\{"([^"]+)",\d+,\s*(\{.*?\})\s*(?:,"[^"]*")?(?:,\d+)?(?=\{"|})', re.DOTALL)

            all_field_matches = list(field_pattern.finditer(fields_block))
            print(f"[DEBUG-PARSE-FIELDS] Найдено {len(all_field_matches)} потенциальных полей.")

            if not all_field_matches:
                print(f"[DEBUG-PARSE-FIELDS] ⚠️ Нет полей, найденных по основному паттерну в блоке {table_name}. Попытка базового поиска.")
                # Если основной паттерн не нашел, попробуем очень базовый, чтобы найти хотя бы имена
                basic_field_name_pattern = re.compile(r'\{"([^"]+)"', re.DOTALL)
                basic_matches = list(basic_field_name_pattern.finditer(fields_block))

                if not basic_matches:
                    print(f"[DEBUG-PARSE-FIELDS] ❌ Базовый поиск полей тоже не дал результатов для {table_name}.")
                    return

                print(f"[DEBUG-PARSE-FIELDS] Базовый поиск нашел {len(basic_matches)} имен полей.")
                for basic_match in basic_matches:
                    raw_field_name = basic_match.group(1).strip()
                    # Для базового поиска мы не можем определить тип точно, но хотя бы зафиксируем имя
                    results.append({
                        'TableName': table_name,
                        'FieldName': "_" + raw_field_name,
                        'FieldType': 'Неизвестно',
                        'FieldTypeCode': 'U',  # Unknown
                        'ReferencedObject': '',
                        'IsTablePart': is_table_part
                    })
                    print(f"[DEBUG-PARSE-FIELDS]  ✓ Добавлено (базовый): {raw_field_name}")
                return

            for field_match in all_field_matches:
                raw_field_name = field_match.group(1).strip()
                field_types_full_block_str = field_match.group(2).strip()  # Это будет '{1,{...}}' или '{6,{...},{...}}'

                print(f"\n[DEBUG-FIELD] Обработка поля: '{raw_field_name}'")
                print(f"[DEBUG-FIELD] Полный блок типов для '{raw_field_name}': {repr(field_types_full_block_str)}")

                # Передаем полный блок типов в новую функцию для детального парсинга
                parse_all_types_for_field(field_types_full_block_str, raw_field_name, table_name, is_table_part, results)

        """Парсим данные схемы, включая основные таблицы и табличные части."""
        results = []
        print("🔍 Первые 1000 символов очищенного контента:")
        print(repr(content[:1000]))
        print("\n" + "=" * 50 + "\n")
        table_start_pattern = re.compile( r'\{"([^"]+)","N",\d+,",\{', re.DOTALL)
        offset = 0
        table_count = 0
        while True:
            match = table_start_pattern.search(content, offset)
            if not match:
                break
            raw_table_name = match.group(1)
            table_name_with_prefix = "_" + raw_table_name
            table_start_pos = match.start()
            block_start_pos = match.end() - 1
            block_end_pos = find_block_end(content, block_start_pos)

            if block_end_pos != -1:
                current_table_block = content[block_start_pos:block_end_pos]
                table_count += 1
                print(f"\n📋 Таблица {table_count}: '{table_name_with_prefix}'")
                print(f"🔍 Длина блока: {len(current_table_block)} символов")
                if len(current_table_block) > 300:
                    print(f"🔍 Начало блока: {repr(current_table_block[:150])}")
                    print(f"🔍 Конец блока: {repr(current_table_block[-150:])}")
                else:
                    print(f"🔍 Весь блок: {repr(current_table_block)}")

                print(f"  Парсинг полей для основной таблицы '{table_name_with_prefix}'")
                parse_fields_in_block(table_name_with_prefix, current_table_block, results, is_table_part=False)

                table_part_pattern = re.compile(r'\{"([^"]+)","I",\d+,"([^"]+)",\{', re.DOTALL)

                table_part_offset = 0
                while True:
                    tp_match = table_part_pattern.search(current_table_block, table_part_offset)
                    if not tp_match:
                        break

                    table_part_name = tp_match.group(1)
                    parent_table_name = tp_match.group(2)

                    full_table_part_name = f"_{parent_table_name}_{table_part_name}"

                    tp_fields_block_start_pos = tp_match.end() - 1

                    tp_fields_block_end_pos = find_block_end(current_table_block, tp_fields_block_start_pos)

                    if tp_fields_block_end_pos != -1:
                        tp_fields_block = current_table_block[tp_fields_block_start_pos:tp_fields_block_end_pos]
                        print(
                            f"\n    📋 Найдена табличная часть: '{full_table_part_name}' (родитель: '{parent_table_name}')")
                        print(f"    🔍 Длина блока полей ТЧ: {len(tp_fields_block)} символов")
                        if len(tp_fields_block) > 300:
                            print(f"    🔍 Начало блока ТЧ: {repr(tp_fields_block[:150])}")
                            print(f"    🔍 Конец блока ТЧ: {repr(tp_fields_block[-150:])}")
                        else:
                            print(f"    🔍 Весь блок ТЧ: {repr(tp_fields_block)}")

                        parse_fields_in_block(full_table_part_name, tp_fields_block, results, is_table_part=True)
                        table_part_offset = tp_fields_block_end_pos
                    else:
                        print(
                            f"    ⚠️ Не удалось найти конец блока полей для табличной части '{table_part_name}'. Пропускаем.")
                        table_part_offset = tp_match.end()

                offset = block_end_pos
            else:
                print(f"⚠️ Не удалось найти конец основного блока для таблицы '{raw_table_name}'. Пропускаем.")
                offset = match.end()

        print(f"\n📈 Итого записей для сохранения: {len(results)}")
        return results
  
    if db_type == 'PG':
        raw_data = cursor.execute(f"SELECT \"serializeddata\" AS \"SerializedData\" FROM public.\"dbschema\" LIMIT 1").fetchone()
    else:
        raw_data = cursor.execute(f"SELECT TOP 1 [SerializedData] FROM [{db_name}].[dbo].[DBSchema]").fetchone()   
    
    if not raw_data or (isinstance(raw_data, str) and not raw_data.strip()):
        print("⚠️ Нет данных или пустой результат из DBSchema 1С.")
        return pd.DataFrame()

    raw_schema_data = raw_data[0] if raw_data else ""
    print(f"📥 Получено данных из DBSchema 1С: {len(str(raw_schema_data))} символов")
    cleaned_content = clean_and_prepare_content(raw_schema_data)
    parsed_data = parse_schema_data(cleaned_content)
    parsed_df = pd.DataFrame(parsed_data)

    return parsed_df

def fetch_DBSchema(cursor):

    if db_type == 'PG':
        cursor.execute("""
        WITH TableCounts AS (
            SELECT 
                c.relname AS TableName,
                c.reltuples AS RowCount
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r' -- Только обычные таблицы
            AND n.nspname = 'public' -- Укажи нужную схему, если не public
        )
        SELECT 
            t.relname AS TableName,
            a.attname   AS ColumnName,
            COALESCE(tc.rowcount, 0) AS RowCount
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON t.oid = a.attrelid
        LEFT JOIN TableCounts tc ON t.relname = tc.TableName
        WHERE t.relkind = 'r'
        AND n.nspname = 'public'
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY TableName, ColumnName;
        """)
    else:
        cursor.execute("""
        WITH TableCounts AS (
            SELECT  t.name AS TableName,
                    SUM(p.rows) AS [RowCount]
            FROM sys.tables AS t
            JOIN sys.partitions AS p ON t.object_id = p.object_id
            WHERE p.index_id IN (0, 1) -- 0 для Heap, 1 для Clustered Index
            GROUP BY t.name
        )
        SELECT  t.name AS TableName,
                c.name AS ColumnName,
                ISNULL(tc.[RowCount], 0) AS [RowCount]
        FROM sys.tables AS t
        JOIN sys.columns AS c ON t.object_id = c.object_id
        LEFT JOIN TableCounts AS tc ON t.name = tc.TableName
        ORDER BY TableName, ColumnName;""")
    
    rows = cursor.fetchall()

    result = pd.DataFrame([{
            'TableName':    table_name,
            'ColumnName':   column_name,
            'RowCount':     row_count,
            'FieldName':    column_name[:-4] if column_name.lower().endswith('rref') or column_name.endswith('tref') else column_name # tref убираем только строчный (PG)
        } for table_name, column_name, row_count in rows])

    return result

def merge_DBSchema(parsed_df, physical_df):
    if parsed_df.empty or physical_df.empty:
        print("❌ Недостаточно данных для объединения. Проверьте предыдущие шаги. Один из DataFrame пуст.")
        return None

    if db_type.lower() == 'pg':
        print("🔧 Применяем маппинг для PostgreSQL...")

        # Создаем маппинги
        table_mapping = {}
        field_mapping = {}

        # Заполняем маппинги из parsed_df
        for _, row in parsed_df.iterrows():
            table_key = str(row['TableName']).lower()
            field_key = str(row['FieldName']).lower()

            if table_key not in table_mapping:
                table_mapping[table_key] = row['TableName']
            if field_key not in field_mapping:
                field_mapping[field_key] = row['FieldName']

        # Применяем маппинг к physical_df
        physical_df = physical_df.copy()

        # Для TableName
        physical_df['TableName'] = physical_df['TableName'].apply(
            lambda x: table_mapping.get(str(x).lower(), x)
        )

        # Для ColumnName - особенная логика с rref
        def map_field_name_(col_name):
            base_name = field_mapping.get(str(col_name).lower(), col_name)
            return base_name

        def map_field_name(col_name, original_col_name):
            base_name = field_mapping.get(str(col_name).lower(), col_name)
            
            # Проверяем оба случая: RRef и TRef
            if (original_col_name.lower().endswith('tref') and
                    not base_name.lower().endswith('tref')):
                return base_name + 'TRef'
            
            return base_name
    
    
        def map_column_name(row):
            column_name = row['FieldName']

            # Если в оригинальном имени было rref, а в маппированном нет - добавляем RRef
            
            try:
                if '_' in row['TableName']:
                    table_base = row['TableName'].rsplit('_')[-2]  # без .lower()!
                    if row['ColumnName'].lower() == f'_{table_base.lower()}_idrref':
                        return f'_{table_base}_IDRRef'  # возвращаем с правильным регистром
            except (IndexError, AttributeError):
                pass  # Игнорируем ошибки           
            
            if (row['ColumnName'].lower().endswith('rref') and
                    not column_name.lower().endswith('rref')):
                return column_name + 'RRef'
            if (row['ColumnName'].lower().endswith('tref') and
                    not column_name.lower().endswith('tref')):
                return column_name + 'TRef'
            if (row['ColumnName'].lower() == '_keyfield'):
                return '_KeyField'    
            return column_name

        # physical_df['FieldName'] = physical_df['FieldName'].apply(map_field_name)
        physical_df['FieldName'] = physical_df.apply(lambda row: map_field_name(row['FieldName'], row['ColumnName']), axis=1)
        physical_df['ColumnName'] = physical_df.apply(map_column_name, axis=1)
        

    # Убираем X1/X2/X3 для логического соединения
    def strip_x_suffix(name):
        return re.sub(r'X\d+$', '', name) if isinstance(name, str) else name

    physical_df['JoinTableName'] = physical_df['TableName'].apply(strip_x_suffix)
    parsed_df['JoinTableName'] = parsed_df['TableName']
    parsed_df = parsed_df.drop(columns=['TableName'])
    
    merged_df = (
        pd.merge(physical_df, parsed_df, on=['JoinTableName', 'FieldName'], how='left')
        .assign(
            ShortTable=lambda x: x["TableName"].str.split("_").str[1],
            FieldType=lambda x: np.where(
                x["FieldType"].notna() & (x["FieldType"] != ''),
                x["FieldType"],
                np.select(
                    [
                        x["ColumnName"] == "_KeyField",
                        x["ColumnName"].str.endswith("TRef"),
                        x["ColumnName"] == "_" + x["ShortTable"] + "_IDRRef"
                    ],
                    ["КлючЗаписи", "ТипОбъекта", "Ссылка"],
                    default=x["FieldType"]
                )
            ),
            ReferencedObject=lambda x: np.where(
                x["ColumnName"] == "_" + x["ShortTable"] + "_IDRRef",
                "_" + x["ShortTable"],
                x["ReferencedObject"]
            )
        )
        .filter(["TableName", "ColumnName", "RowCount", "FieldType", "ReferencedObject"])
    )

    save_to_csv(merged_df, "DBSchema")
    print("🎉 Объединение завершено успешно!")
    return merged_df



def fetch_DBNmes(cursor):
    if db_type == 'PG':
        cursor.execute("SELECT \"binarydata\" AS \"BinaryData\" FROM public.\"params\" WHERE \"filename\" = 'DBNames'")  
    else:
        cursor.execute("SELECT [BinaryData] FROM [Params] WHERE [FileName] = N'DBNames'")
        
    row = cursor.fetchone()
    result = []

    if row:
        binary_data = row[0]
        try:
            decompressed = zlib.decompress(binary_data, -zlib.MAX_WBITS)
            text = decompressed.decode('utf-8', errors='ignore')

            if text:
                pattern = re.compile(r"\{([\da-fA-F\-]+),\"(\w+)\",(\d+)\}")
                matches = pattern.findall(text)

                result = pd.DataFrame([{
                    'UUID': uuid,
                    'Object': f"_{obj_type}{obj_id}",
                    'Object_type': obj_type,
                    'Object_id': obj_id
                } for uuid, obj_type, obj_id in matches])

        except zlib.error as e:
            print(f"⚠️ Ошибка декомпрессии DBNames: {e}")
        except Exception as e:
            print(f"⚠️ Ошибка обработки DBNames: {e}")

    result = pd.DataFrame(result)
    save_to_csv(result, "Dbnames")

    return result

def fetch_Config(cursor, dbnames_df):
    if db_type == 'PG':
        cursor.execute(f"""SELECT "filename" AS "FileName", "binarydata" AS "BinaryData" FROM public."config" """)
    else:
        cursor.execute(f"""SELECT [FileName], [BinaryData] FROM [{db_name}].[dbo].[Config] """)
        
    rows = cursor.fetchall()

    uuid_set = set(dbnames_df['UUID'].str.lower())  # Готовим список UUID для фильтрации таблицы config
    print(f"✅ Создан uuid_set с {len(uuid_set)} элементами для фильтрации Config.")

    result = []
    for row in rows:
        if row.FileName.lower() not in uuid_set:
            continue
        try:
            decompressed = zlib.decompress(row.BinaryData, -zlib.MAX_WBITS)
            text = decompressed.decode('utf-8', errors='ignore')
        except Exception as e:
            text = f"<<ERROR: {e}>>"

        result.append({
            'FileName': row.FileName,
            'BinaryData': text
        })

    result = pd.DataFrame(result)
    save_to_csv(result, "Config")

    return result

def process_Enums(dbnames_df, config_df):
    """Извлекает перечисления и их значения с синонимами из DBNames и Config"""
    enums_df = dbnames_df[dbnames_df['Object_type'] == 'Enum'].copy()
    enums_df = pd.merge(enums_df, config_df, how='left', left_on='UUID', right_on='FileName')

    def extract_enum_info(row):
        data = row['BinaryData']
        uuid = row['UUID']
        enum_name = enum_synonym = values_str = None
        try:
            match_name = re.search(re.escape(uuid) + r'},"([^"]+)"', data)
            if match_name:
                enum_name = match_name.group(1)
            match_syn = re.search(re.escape(uuid) + r'},"[^"]+",.*?"ru"\s*,\s*"([^"]+)"', data)
            if match_syn:
                enum_synonym = match_syn.group(1)
            start = data.find('bee0a08c-07eb-40c0-8544-5c364c171465')
            if start != -1:
                brace_pos = data.find('{', start)
                if brace_pos != -1:
                    values_str = data[brace_pos:]
        except Exception:
            pass
        return pd.Series([enum_name, enum_synonym, values_str])

    enums_df[['EnumName', 'EnumSynonym', 'Values']] = enums_df.apply(extract_enum_info, axis=1)

    def parse_enum_value_block(block):
        try:
            value_name = re.search(r'"([^"]+)"', block).group(1).replace('""', '"').strip('"')
        except Exception:
            value_name = None
        try:
            value_synonym = re.search(r'"ru"\s*,\s*"([^"]+)"', block).group(1).replace('""', '"').strip('"')
        except Exception:
            value_synonym = None
        if not value_synonym and value_name:
            value_synonym = value_name
        return value_name, value_synonym

    exploded_rows = []
    for _, row in enums_df.iterrows():
        values = row['Values']
        if not values:
            continue
        for i, val in enumerate(values.split('},0},')):
            value_name, value_synonym = parse_enum_value_block(val.strip('{}"'))
            exploded_rows.append({
                'Перечисление': row['EnumName'],
                'SQL name': row['Object'],
                'Значение': value_name,
                'Синоним': value_synonym,
                'Порядок': i
            })

    enum_df = pd.DataFrame(exploded_rows)
    save_to_csv(enum_df, "Enums")
    return enum_df


def process_Tables(dbnames, config, dbschema):
    slovar = """
    Синоним	ТипОбъекта	Объект	Назначение
    Бизнес-процессы	БизнесПроцесс	BPr	Основная
    Бизнес-процессы	БизнесПроцесс	BPrChngR	РегистрацияИзменений
    Бизнес-процессы	БизнесПроцесс	BPrPoints	ТочкиМаршрута
    Бизнес-процессы	БизнесПроцесс	BPrVT	ТабличнаяЧасть

    Планы видов расчета	ПланВидовРасчета	CKinds	Основная
    Планы видов расчета	ПланВидовРасчета	CKindsBaseCK	ТабличнаяЧасть
    Планы видов расчета	ПланВидовРасчета	CKindsLeadingCK	ТабличнаяЧасть
    Планы видов расчета	ПланВидовРасчета	CKindsDisplacedCK	ТабличнаяЧасть
    Планы видов расчета	ПланВидовРасчета	CKindsVT	ТабличнаяЧасть
    
    Боты	Бот	Bots	БотыСистемыВзаимодействия
    Документы	Документ	Document	Основная
    Документы	Документ	DocumentChngR	РегистрацияИзменений
    Документы	Документ	DocumentVT	ТабличнаяЧасть
    Журнал документов	ЖурналДокументов	DocumentJournal	Основная
    Задачи	Задача	Task	Основная
    Задачи	Задача	TaskVT	ТабличнаяЧасть
    Задачи	Задача	TaskChngR	РегистрацияИзменений
    Константы	Константа	Const	Константа
    Константы	Константа	ConstChngR	РегистрацияИзмененийКонстанты
    Хранилища	Хранилище	DynListSettings	ХранилищеПользовательскихНастроекДинамическихСписков
    Хранилища	Хранилище	UsersWorkHistory	ИсторияРаботыПользователей
    Хранилища	Хранилище	ConfigChngR	РегистрацияИзмененийКонфигурации
    Хранилища	Хранилище	ConfigChngRExtProps	РегистрацияИзмененийВнешнихСвойствКонфигурации
    Хранилища	Хранилище	RepVarSettings	ХранилищеВариантовОтчетов
    Хранилища	Хранилище	FrmDtSettings	ХранилищеНастроекДанныхФорм
    Хранилища	Хранилище	CommonSettings	ХранилищеОбщихНастроек
    Хранилища	Хранилище	RepSettings	ХранилищеПользовательскихНастроекОтчетов
    Хранилища	Хранилище	SystemSettings	ХранилищеСистемныхНастроек
    Перечисления	Перечисление	Enum	Основная
    Планы видов расчета	ПланВидовРасчета	CKinds	Основная
    Планы видов расчета	ПланВидовРасчета	CKindsOpt	НастройкиПлановВидовРасчетов
    Планы видов расчета	РегистрРасчета	CRgActP	ПериодыДействия
    Планы видов расчета	ПланВидовРасчета	CKindsDN	ПорядокВытеснения
    Планы видов расчета	ПланВидовРасчета	CKindsChngR	РегистрацияИзменений
    Планы видов характеристик	ПланВидовХарактеристик	Chrc	Основная
    Планы видов характеристик	ПланВидовХарактеристик	ChrcChngR	РегистрацияИзменений
    Планы видов характеристик	ПланВидовХарактеристик	ChrcOpt	НастройкиПлановВидовХарактеристик
    Планы видов характеристик	ПланВидовХарактеристик	ChrcSInf	ИнициализированныеПредопределенныеДанныеПланаВидовХарактеристик
    Планы видов характеристик	ПланВидовХарактеристик	ChrcVT	ТабличнаяЧасть
    Планы обмена	ПланОбмена	Node	Основная
    Планы обмена	ПланОбмена	NodeVT	ТабличнаяЧасть
    Планы счетов	ПланСчетов	Acc	Основная
    Планы счетов	ПланСчетов	AccVT	ТабличнаяЧасть
    Планы счетов	ПланСчетов	AccChngR	РегистрацияИзменений
    Планы счетов	ПланСчетов	AccExtDim	ВидыСубконто
    Планы счетов	ПланСчетов	AccSInf	ИнициализированныеПредопределенныеДанныеПланаСчетов
    Планы счетов	ПланСчетов	AccOpt	НастройкиПлановСчетов
    Последовательности	Последовательность	Seq	Основная
    Последовательности	Последовательность	SeqB	ГраницыПоследовательности
    Последовательности	Последовательность	SeqChngR	РегистрацияИзменений
    Регистры бухгалтерии	РегистрБухгалтерии	AccRgED	ЗначенияСубконто
    Регистры бухгалтерии	РегистрБухгалтерии	AccRgCT	ИтогиМеждуСчетами
    Регистры бухгалтерии	РегистрБухгалтерии	AccRgAT	ИтогиПоСчетамССубконто
    Регистры бухгалтерии	РегистрБухгалтерии	AccRgOpt	НастройкиХраненияИтоговРегистраБухгалтерии
    Регистры бухгалтерии	РегистрБухгалтерии	AccRg	Основная
    Регистры бухгалтерии	РегистрБухгалтерии	AccRgChngR	РегистрацияИзменений
    Регистры накопления	РегистрНакопления	AccumRgT	Итоги
    Регистры накопления	РегистрНакопления	AccumRgOpt	НастройкиХраненияИтоговРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgTn	Обороты
    Регистры накопления	РегистрНакопления	AccumRg	Основная
    Регистры накопления	РегистрНакопления	AccumRgChngR	РегистрацияИзменений
    Регистры накопления	РегистрНакопления	AccumRgAggDicth	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDims	КодыИзмеренийАгрегатовРегистровНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggGridK	СписокАгрегатовРегистровНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggOptK	НастройкиРежимаАгрегатовРегистровНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictah	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictbh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictch	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictdh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDicteh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictfh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictgh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDicthh	СловарьИзмеренияРегистраНакопления
    Регистры накопления	РегистрНакопления	AccumRgAggDictih	СловарьИзмеренияРегистраНакопления
    
    Регистры накопления	РегистрНакопления	AccumRgBfK	БуферОборотов
    Регистры накопления	РегистрНакопления	AccumRgDlK	НовыеОбороты
    Регистры накопления	РегистрНакопления	AccumRgSt	СтатистикаЗапросов
    Регистры расчета	РегистрРасчета	CRg	Основная
    Регистры расчета	РегистрРасчета	CRgChngR	РегистрацияИзменений
    Регистры сведений	РегистрСведений	InfoRgSF	ИтогиСрезПервых
    Регистры сведений	РегистрСведений	InfoRgSL	ИтогиСрезПоследних
    Регистры сведений	РегистрСведений	InfoRgOpt	НастройкиХраненияИтоговРегистраСведений
    Регистры сведений	РегистрСведений	InfoRg	Основная
    Регистры сведений	РегистрСведений	InfoRgChngR	РегистрацияИзменений
    Регламентные задания	РегламентноеЗадание	ScheduledJobs	РегламентныеЗадания
    Справочники	Справочник	Reference	Основная
    Справочники	Справочник	ReferenceChngR	РегистрацияИзменений
    Справочники	Справочник	ReferenceVT	ТабличнаяЧасть
    Справочники	Справочник	RefOpt	НастройкиСправочников
    Справочники	Справочник	RefSInf	ИнициализированныеПредопределенныеДанныеСправочника
    """
    # Используем io.StringIO для чтения строки как файла
    slovar = pd.read_csv(io.StringIO(slovar), sep='\t')  # sep='\t' если разделитель - табуляция
    # slovar = pd.read_excel(r"D:\QlikView\BIN\Meta\Словарь.xlsx", sheet_name='Sheet1')  # Синоним, ТипОбъекта, Объект, Назначение
    print(slovar.columns)
    # --- Вспомогательные функции ---
    uuid_map = dict(zip(dbnames['Object'], dbnames['UUID']))
    type_map = dict(zip(dbnames['Object'], dbnames['Object_type']))

    get_master_uuid = lambda name: uuid_map.get(name) or uuid_map.get('_' + name.split('_')[1]) if '_' in name else None
    get_tmp_slave_uuid = lambda name: uuid_map.get('_' + name.split('_')[-1]) if '_' in name else None
    
    get_table_type = lambda name: type_map.get('_' + name.split('_')[-1]) if '_' in name else None

    # --- утилиты в стиле Qlik ---
    def subfield(s: str, token: str, n: int) -> str:
        """Вернуть n-й кусок после разделения по token (1-индексация как в Qlik)."""
        if s is None:
            return ''
        parts = str(s).split(token)
        return parts[n - 1] if n - 1 < len(parts) else ''

    def purge_chars(s: str) -> str:
        """Аналог PurgeChar(..., '\"{}')"""
        if s is None:
            return ''
        return re.sub(r'["{}]', '', str(s)).strip()

    def nth_comma_field(s: str, n: int) -> str:
        """Вернуть n-е поле по запятым (1-индексация)."""
        if s is None:
            return ''
        parts = str(s).split(',')
        return parts[n - 1] if n - 1 < len(parts) else ''

    # --- замены функций извлечения ---

    def extract_tmp_master_name(row):
        """
        Qlik: PurgeChar(SubField(SubField(BinaryData, MasterUUID, 2), ',', 2), '\"{}')
        """
        bin_str = '' if row.get('BinaryData') is None else str(row['BinaryData'])
        uuid = '' if row.get('MasterUUID') is None else str(row['MasterUUID']).strip('"')
        if not uuid:
            return ''
        tail = subfield(bin_str, uuid, 2)  # всё после первого вхождения UUID
        name = nth_comma_field(tail, 2)  # 2-е поле по запятой
        return purge_chars(name) or None

    def extract_master_synonym(binary_data, master_uuid):
        """
        Qlik: PurgeChar(SubField(SubField(BinaryData, '\"ru\"', 2), ',', 2), '\"{}')
        """
        bin_str = '' if binary_data is None else str(binary_data)
        tail = subfield(bin_str, '"ru"', 2)
        syn = nth_comma_field(tail, 2)
        val = purge_chars(syn)
        return val or None

    def extract_tmp_slave_name(row):
        """
        Qlik: PurgeChar(SubField(SubField(BinaryData, SlaveUUID & '},\"', 2), ',', 1), '\"{}')
        """
        bin_str = '' if row.get('BinaryData') is None else str(row['BinaryData'])
        suuid = '' if row.get('SlaveUUID') is None else str(row['SlaveUUID']).strip('"')
        if not suuid:
            return ''
        marker = suuid + '},"'  # ровно как в Qlik: SlaveUUID & '},\"'
        tail = subfield(bin_str, marker, 2)
        name = nth_comma_field(tail, 1)
        val = purge_chars(name)
        return val or None

    def extract_tmp_slave_synonym(row):
        """
        Qlik: PurgeChar(SubField(SubField(SubField(BinaryData, SlaveUUID & '},\"', 2), '\"ru\"', 2), ',', 2), '\"{}')
        """
        bin_str = '' if row.get('BinaryData') is None else str(row['BinaryData'])
        suuid = '' if row.get('SlaveUUID') is None else str(row['SlaveUUID']).strip('"')
        if not suuid:
            return ''
        marker = suuid + '},"'
        tail1 = subfield(bin_str, marker, 2)
        tail2 = subfield(tail1, '"ru"', 2)
        syn = nth_comma_field(tail2, 2)
        val = purge_chars(syn)
        return val or None

    def extract_props_uuids(binary_data, sql_name):
        """
        Qlik: PurgeChar(SubField(SubField(BinaryData, Chr(10), 2), ',', If(Sql_name like('*enum*'), 2, 4)), '\"{}')
        т.е. берем 2-ю строку, затем 2-е поле по запятой для enum, иначе 4-е.
        """
        s = '' if binary_data is None else str(binary_data)
        # 2-я «строка» — всё после первого \n
        tail_line2 = subfield(s, '\n', 2)
        # выбор индекса по условию
        idx = 2 if (isinstance(sql_name, str) and 'enum' in sql_name.lower()) else 4
        field = nth_comma_field(tail_line2, idx)
        return purge_chars(field) or ''

    def compute_slave_name(row):
        # 1) Явное имя из бинарей, если оно есть и отличается от мастера
        if row.get('tmpSlaveName') and row['tmpSlaveName'] != row.get('MasterName'):
            return row['tmpSlaveName']

        # 2) Спец-случай для AccRgAT
        if str(row.get('TableType', '') or '').replace('_', '') == 'AccRgAT':   
            return str(row.get('dbNamesType', '')).replace('AccRgAT', row.get('ObjPurpose')) #'ИтогиПоСчетамССубконото')

        # 3) Иначе — цель из словаря, если это не базовые типы
        op = row.get('ObjPurpose')
        if op and op not in ['Основная', 'ТабличнаяЧасть', 'Константа']:
            return op

        return None
        
    resolve_master_name = lambda row: row['tmpMasterName'] or row['ObjPurpose'] or row['Sql_name']
    
    compute_slave_name_old = lambda row: row['tmpSlaveName'] if row['tmpSlaveName'] and row['tmpSlaveName'] != row[
        'MasterName'] else (
        row['ObjPurpose'] if row['ObjPurpose'] not in ['Основная', 'ТабличнаяЧасть', 'Константа'] else None)
    compute_slave_synonym = lambda row: row['tmpSlaveSynonym'] if row['tmpSlaveName'] and row['tmpSlaveName'] != row[
        'MasterName'] else None

    # --- Основная обработка ---
    dbschema['Sql_name'] = dbschema['TableName']
    dbschema['MasterUUID'] = dbschema['TableName'].apply(get_master_uuid)
    dbschema['tmpSlaveUUID'] = dbschema['TableName'].apply(get_tmp_slave_uuid)
    dbschema['dbNamesType'] = dbschema['TableName'].apply(get_table_type)                                                     # тип таблицы из dbNames
    dbschema['TableType'] = dbschema['TableName'].str.replace(r'[_\d]', '', regex=True)
    dbschema['SlaveUUID'] = dbschema.apply(lambda x: x['tmpSlaveUUID'] if x['tmpSlaveUUID'] != x['MasterUUID'] else '',
                                           axis=1)

    t = dbschema[
        ['Sql_name', 'TableName', 'MasterUUID', 'tmpSlaveUUID', 'SlaveUUID', 'TableType', 'RowCount', 'dbNamesType']].drop_duplicates()
    t = t.merge(config.rename(columns={'FileName': 'MasterUUID'}), how='left', on='MasterUUID')
    t = t.merge(slovar.rename(
        columns={'Синоним': 'ObjSynonym', 'Назначение': 'ObjPurpose', 'Объект': 'TableType', 'ТипОбъекта': 'Object'}),
                how='left', on='TableType')

    t['BinaryData'] = t['BinaryData'].fillna('')
    t['tmpMasterName'] = t.apply(extract_tmp_master_name, axis=1)
    t['MasterSynonym'] = t.apply(lambda r: extract_master_synonym(r['BinaryData'], r['MasterUUID']), axis=1)
    t['tmpSlaveName'] = t.apply(extract_tmp_slave_name, axis=1)
    t['tmpSlaveSynonym'] = t.apply(extract_tmp_slave_synonym, axis=1)
    t['MasterName'] = t.apply(resolve_master_name, axis=1)
    t['SlaveName'] = t.apply(compute_slave_name, axis=1)
    
    t['SlaveSynonym'] = t.apply(compute_slave_synonym, axis=1)

    t['TableName'] = t.apply(
        lambda r: f"{r['MasterName']}.{r['SlaveName']}" if pd.notna(r['SlaveName']) and r['SlaveName'] != '' else r[
            'MasterName'], axis=1)
    t['TableSynonym'] = t.apply(
        lambda r: r['SlaveSynonym'] if pd.notna(r['SlaveSynonym']) and r['SlaveSynonym'] != '' else r['MasterSynonym'],
        axis=1)
    t['ИмяТаблицыQV'] = t.apply(
        lambda r: f"{r['Object']}.{r['TableName']}" if pd.notna(r['Object']) and r['Object'] != '' else r['Sql_name'],
        axis=1)


    def normalize_buh_totals(name):
        if not isinstance(name, str):
            return name

        parts = name.split('.')

        # РегистрБухгалтерии.*.ИтогиПоСчетамССубконто0
        if len(parts) >= 3 and parts[0] == 'РегистрБухгалтерии' \
           and parts[2].startswith('ИтогиПоСчетамССубконто0'):
            parts[2] = 'Остатки'

        return '.'.join(parts)
    t['ИмяТаблицыQV'] = t['ИмяТаблицыQV'].apply(normalize_buh_totals)

    t['Object'] = t['Object'].fillna('').replace('', 'Системные таблицы')
    t['Представление'] = t['ИмяТаблицыQV']
    t['PropsUUIDs'] = t.apply(lambda r: extract_props_uuids(r['BinaryData'], r['Sql_name']), axis=1)




    result = t.rename(columns={
        'Object': 'Объект',
        'ObjPurpose': 'Назначение',
        'TableSynonym': 'Синоним',
        'RowCount': 'КоличествоСтрок',
        'Sql_name': 'ИмяХранения',
        'MasterUUID': 'MasterUUID',
        'PropsUUIDs': 'PropsUUIDs',
        
        'dbNamesType': 'dbNamesType',
        'SlaveName': 'SlaveName'
    })[['Объект', 'Назначение', 'ИмяТаблицыQV', 'Представление', 'Синоним', 'КоличествоСтрок',
        'ИмяХранения', 'MasterUUID', 'PropsUUIDs', 
        'dbNamesType', 'SlaveName']].drop_duplicates()

    # --- Сохранение финальной таблицы ---
    save_to_csv(result, "Tables")

    return result

def process_Fields(dbnames,config,schema,tables):
    slovar = """
    ColumnName	ИмяПоля	Синоним
    _NumberPrefix	Префикс	Префикс
    _DimKey	КлючИзмерения	Ключ измерения
    _RecordKey  	КлючЗаписи	 Ключ записи
    _DataSeparationUse	_DataSeparationUse	_DataSeparationUse
    _AccountRRef	Счет	Счет
    _AccountDtRRef	СчетДт	Счет Дт
    _AccountCtRRef	СчетКт	Счет Кт
    _IsMetadata	Метаданные	Метаданные
    _Active	Активность	Активность
    _BusinessProcessRRef	БизнесПроцесс	Бизнес процесс
    _BusinessProcess_RRRef	БизнесПроцесс	Бизнес процесс
    _BusinessProcess_RTRef	БизнесПроцесс	Бизнес процесс
    _BusinessProcess_TYPE	БизнесПроцесс	Бизнес процесс
    _Code	Код	Код
    _Completed	Завершен	Завершен
    _Correspond	Корреспондирующее	Корреспондирующее
    _Date_Time	Дата	Дата
    _Description	Наименование	Наименование
    _DimIsMetadata	Предопределенное	Предопределенное
    _DimKindRRef	ВидСубконто	Вид субконто
    _DocumentRRef	Ссылка	Ссылка
    _DocumentTRef	Ссылка	Ссылка
    _EDHashDt	ХешДт	Хеш Дт
    _EDHashCt	ХешКт	Хеш Кт
    _EnumOrder	Порядок	Порядок
    _Executed	Выполнена	Выполнена
    _Folder	ЭтоГруппа	Это группа
    _HeadTaskRRef	ВедущаяЗадача	Ведущая задача
    _IDRRef	Ссылка	Ссылка
    _KeyField	КлючЗаписи	Ключ записи
    _Kind	Вид	Вид
    _KindRRef	ВидСубконто	Вид субконто
    _LineNo	НомерСтроки	Номер строки
    _Marked	ПометкаУдаления	Пометка удаления
    _MessageNo	НомерСообщения	Номер сообщения
    _Name	Наименование	Наименование
    _NodeRRef	Узел	Узел
    _NodeTRef	Узел	Узел
    _Number	Номер	Номер
    _OffBalance	Забалансовый	Забалансовый
    _OrderField	Порядок	Порядок
    _OwnerID_RRRef	Владелец	Владелец
    _OwnerID_RTRef	Владелец	Владелец
    _OwnerID_TYPE	Владелец	Владелец
    _OwnerIDRRef	Владелец	Владелец
    _ParentIDRRef	Родитель	Родитель
    _Period	Период	Период
    _PointRRef	ТочкаМаршрута	Точка маршрута
    _Point_RRRef	ТочкаМаршрута	Точка маршрута
    _Point_RTRef	ТочкаМаршрута	Точка маршрута
    _Point_TYPE	ТочкаМаршрута	Точка маршрута
    _Posted	Проведен	Проведен
    _PredefinedID	ИмяПредопределенныхДанных	Имя предопределенных данных
    _ReceivedNo	НомерПринятого	Номер принятого
    _Recorder_RRRef	Регистратор	Регистратор
    _Recorder_RTRef	Регистратор	Регистратор
    _Recorder_TYPE	Регистратор	Регистратор
    _RecorderRRef	Регистратор	Регистратор
    _RecorderTRef	Регистратор	Регистратор
    _RecordKind	ВидДвижения	ВидДвижения
    _RoutePointOrder	Порядок	Порядок
    _SentNo	НомерОтправленного	Номер отправленного
    _Started	Стартован	Стартован
    _TurnoverOnly	ТолькоОбороты	Только обороты
    _Type	ТипЗначения	Тип значения
    _Version	ВерсияДанных	Версия данных
    """
    # Используем io.StringIO для чтения строки как файла
    #slovar = pd.read_csv(io.StringIO(slovar), sep='\t')  # sep='\t' если разделитель - табуляция

    slovar = pd.read_csv(io.StringIO(slovar), sep='\t')
    slovar.columns = slovar.columns.str.strip()
    slovar.rename(columns=lambda x: x.strip(), inplace=True)
    slovar['ColumnName'] = slovar['ColumnName'].str.strip()

    print(slovar.columns)

    uuid_map = dict(zip(dbnames['Object'], dbnames['UUID']))
    table_map = dict(zip(tables['ИмяХранения'], tables['ИмяТаблицыQV']))

    tables = tables[['Объект', 'Назначение', 'ИмяТаблицыQV', 'ИмяХранения', 'MasterUUID']].copy()
    schema = schema[['TableName', 'ColumnName', 'FieldType', 'ReferencedObject']].copy()

    def transform_column_name(name):
        if name.count('_') == 2:
            return '_' + name.split('_')[1]
        if name.startswith('_Fld'):
            if name.endswith('Dt') or name.endswith('Ct'):
                return name[:-2]
            if name.endswith('CtRRef') or name.endswith('DtRRef'):
                return name[:-6]
            if 'RRef' in name:
                return name.replace('RRef', '')
        return name

    schema['F1'] = schema['ColumnName'].apply(transform_column_name)
    schema['FieldType'] = schema.apply(
        lambda r: table_map.get(r['ReferencedObject'], r.get('FieldType', r['ReferencedObject'])),
        axis=1
    )
    schema['FieldUUID'] = schema['F1'].map(uuid_map)

    df = tables.merge(
        schema[['TableName', 'ColumnName', 'FieldType', 'ReferencedObject', 'FieldUUID']],
        left_on='ИмяХранения', right_on='TableName', how='left'
    )

    config_reduced = config[['FileName', 'BinaryData']]
    df = df.merge(config_reduced, left_on='MasterUUID', right_on='FileName', how='left').rename(columns={'BinaryData': 'BinaryData_1'})
    df = df.merge(config_reduced, left_on='FieldUUID', right_on='FileName', how='left').rename(columns={'BinaryData': 'BinaryData_2'})

    df = df.merge(slovar, on='ColumnName', how='left')

    def extract_fieldname(binary, uuid):
        try:
            uuid_str = str(uuid) if pd.notna(uuid) else ''
            binary_str = str(binary) if pd.notna(binary) else ''
            match = re.search(re.escape(uuid_str) + r'},"([^",}]*)', binary_str)
            return match.group(1).translate(str.maketrans('', '', '"{}')) if match else ''
        except Exception:
            return ''

    def extract_fieldsynonym(binary, uuid):
        try:
            if pd.isna(binary) or pd.isna(uuid):
                return ''
            text = str(binary)
            key = f"{uuid}}},\""
            part = text.split(key, 1)
            if len(part) < 2:
                return ''
            second = part[1].split('"ru"', 1)
            if len(second) < 2:
                return ''
            third = second[1].split(',', 2)
            if len(third) < 3:
                return ''
            value = third[1]
            return value.strip('"{} ')
        except Exception:
            return ''

    df['FieldName_1'] = df.apply(lambda r: extract_fieldname(r['BinaryData_1'], r['FieldUUID']), axis=1)
    df['FieldName_2'] = df.apply(lambda r: extract_fieldname(r['BinaryData_2'], r['FieldUUID']), axis=1)
    df['FieldSynonym_1'] = df.apply(lambda r: extract_fieldsynonym(r['BinaryData_1'], r['FieldUUID']), axis=1)
    df['FieldSynonym_2'] = df.apply(lambda r: extract_fieldsynonym(r['BinaryData_2'], r['FieldUUID']), axis=1)

    # Приоритет строго по Qlik
    df['FieldName_tmp'] = df['ИмяПоля'].where(df['ИмяПоля'].notna() & (df['ИмяПоля'] != ''), df['FieldName_1'])
    df['FieldName_tmp'] = df['FieldName_tmp'].where(df['FieldName_tmp'].notna() & (df['FieldName_tmp'] != ''), df['FieldName_2'])

    df['FieldSynonym_tmp'] = df['Синоним'].where(df['Синоним'].notna() & (df['Синоним'] != ''), df['FieldSynonym_1'])
    df['FieldSynonym_tmp'] = df['FieldSynonym_tmp'].where(df['FieldSynonym_tmp'].notna() & (df['FieldSynonym_tmp'] != ''), df['FieldSynonym_2'])

    df['FieldName'] = df.apply(lambda r: 'НомерСтроки' if '_LineNo' in r['ColumnName'] else r['FieldName_tmp'], axis=1)
    df['FieldSynonym'] = df.apply(lambda r: 'Номер строки' if '_LineNo' in r['ColumnName'] else r['FieldSynonym_tmp'], axis=1)

    def subfield(val, delim, n):
        parts = str(val).split(delim)
        return parts[n-1] if len(parts) >= n else ''

    def calc_fieldname(row):
        col = str(row.get('ColumnName', ''))
        subtype = subfield(col, '_', 3)
        field_type = str(row.get('FieldType', ''))
        field_name = str(row.get('FieldName', ''))
        table_name = str(row.get('ИмяТаблицыQV', ''))


    
        # Обработка всех случаев замены Value → Субконто
        def process_value(s):
            parts = s.split('_')
            if len(parts) > 1:
                value_part = parts[1]
                value = re.sub(r'[0-9]', '', value_part);
                if value == 'Value':
                    return value_part.replace('Value', 'Субконто')
                if value in ['Turnover','TurnoverCt','TurnoverDt']: 
                    return f"{field_name}.{value}".replace('Turnover', 'Оборот').replace('Dt', 'Дт').replace('Ct', 'Кт')
            return None
      
        # --- базовая логика ---
        if field_type in ['Ссылка', 'ТипОбъекта'] or \
           (col.count('_') == 2 and subtype in ['L','N','S','T','RRRef','TYPE','RTRef']):

            if field_name.strip():
                result = process_value(col) or field_name
                result = f"{result}.{field_type}"
            else:
                result = col

        elif field_name == subfield(table_name, '.', 2) and field_type != ".".join(table_name.split(".")[:2]):
            result = col[1:] if col.startswith('_') else col

        elif field_name.strip():
            result = process_value(col) or field_name

        else:
            result = process_value(col) or (col[1:] if col.startswith('_') else col)

        if 'EDHash' in result:
            return result
            
        # ✅ --- ДОБАВЛЯЕМ Дт / Кт ---
        if col.endswith('Dt') and not result.endswith('Дт'):
            result += 'Дт'

        if col.endswith('Ct') and not result.endswith('Кт'):
            result += 'Кт'

        if col.endswith('DtRRef'):
            if '.Ссылка' in result and not 'Дт.Ссылка' in result:
                result = result.replace('.Ссылка', 'Дт.Ссылка')
            elif not result.endswith('Дт'):
                result += 'Дт'

        if col.endswith('CtRRef'):
            if '.Ссылка' in result and not 'Кт.Ссылка' in result:
                result = result.replace('.Ссылка', 'Кт.Ссылка')
            elif not result.endswith('Кт'):
                result += 'Кт'
            
        return result


    def calc_synonym(row):
        col = str(row.get('ColumnName', ''))

        subtype = subfield(col, '_', 3)
        syn = row['FieldSynonym'] or subfield(row['ИмяПоля'], '.', 1)

        if subtype in ['L','N','S','T','TYPE','RTRef'] or row['FieldType'] in ['ТипЗначения', 'ТипОбъекта']:
            syn = f"{syn} ({row['FieldType']})" \
                .replace('ТипЗначения', 'Тип значения') \
                .replace('ТипОбъекта', 'Тип объекта')

        elif subfield(row['ИмяПоля'], '.', 2) in ['Оборот','ОборотДт','ОборотКт']:
#            syn = f"{syn} ({subfield(row['ИмяПоля'], '.', 2)})" \
            syn = f"{syn} {subfield(row['ИмяПоля'], '.', 2)}" \
               .replace('Дт', ' Дт') \
                .replace('Кт', ' Кт') \
                  .replace('Оборот', '(Оборот)')

        if 'EDHash' in syn:
            return syn
            
        # ✅ --- добавляем Дт / Кт ---
        if (col.endswith('Dt') or col.endswith('DtRRef')) and ' Дт' not in syn:
            syn += ' Дт'

        if (col.endswith('Ct') or col.endswith('CtRRef')) and ' Кт' not in syn:
            syn += ' Кт'

        return syn
    
    df['ИмяПоля'] = df.apply(calc_fieldname, axis=1)
    df['Синоним'] = df.apply(calc_synonym, axis=1)
    df['ИмяХранения'] = df['ColumnName']
    df['ТипРеквизита'] = df['FieldType']
    df['ИмяХраненияТипа'] = df['ReferencedObject']
    
     
    def calc_представление(row):
        field = row['ИмяПоля']
        table = row['ИмяТаблицыQV']
        obj = row['Объект']
        raw = row['ИмяХранения']
        
        parts = table.split('.')

        if field == subfield(table, '.', 2) and '.'.join(table.split('.')[:2]) == row['ТипРеквизита']:
            return f"{obj}.{field}.Ссылка"
        elif field == subfield(table, '.', 2):
            return f"{table}.{raw}" 
        elif 'rref' in str(raw).lower() and 'Ссылка' not in field:
            return f"{table}.{field}.Ссылка"
        # elif field == subfield(table, '.', 2):
            # return f"{table}.{raw}" 
        else:
            return f"{table}.{field}"

    df['Представление'] = df.apply(calc_представление, axis=1)
    df['ИмяПоляQV'] = df['Представление'].apply(lambda s: s.split('.', 1)[1] if pd.notna(s) and '.' in s else '')



    # Создаем условие для отбора строк
    mask = df["Представление"] == df["ИмяТаблицыQV"].apply(
        lambda table: table.replace(subfield(table, '.', 3), 'Ссылка')
    )

    # Заменяем "Представление" на "ИмяТаблицыQV" + ".Ссылка" для найденных строк
    df.loc[mask, "Представление"] = df.loc[mask, "ИмяТаблицыQV"] + ".Ссылка"
    

          
    df = df[['Объект', 'ИмяТаблицыQV', 'Назначение', 'ИмяПоля', 'Синоним', 'ИмяПоляQV', 'Представление', 'ТипРеквизита', 'ИмяХраненияТипа', 'ИмяХранения', 'FieldName', 'FieldName_1', 'FieldName_2']].copy()

    save_to_csv(df, "Fields")
    return df

def process_Final(fields, tables):
    """Сохраняем данные в CSV"""
    df = pd.concat([fields, tables], ignore_index=True)
    df = df.drop(['MasterUUID', 'PropsUUIDs'], axis=1) # Удаление столбцов 
    save_to_csv(df, "Final")

    return df

def save_to_csv(data, df_name="Данные"):
    """Сохраняем данные в CSV"""
    output_path = csv_path +"\\"+ df_name + r".csv"

    if isinstance(data, pd.DataFrame):
        if data.empty:
            print(f"⚠️ Нет данных для сохранения в {output_path}")
            return False
        df = data
    else:
        if not data:
            print(f"⚠️ Нет данных для сохранения в {output_path}")
            return False
        df = pd.DataFrame(data)

    df.to_csv(output_path, index=False, encoding="utf-8-sig", sep=',', header=True, escapechar='\\')

    print(f"✅ {df_name} сохранены в: {output_path}")
    print(f"📊 Количество записей: {len(df)}")
    print(f"📋 Столбцы: {list(df.columns)}")
    print(f"\n🔍 Первые 5 записей {df_name}:")
    print(df.head())

    return True
# --- Основной блок выполнения ---
if __name__ == "__main__":
    try:
        print("🚀 Начинаем обработку данных из 1С и физической схемы БД...")

        # Открываем соединение ОДИН РАЗ
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        DBSchema_parsed     = parse_DBSchema(cursor)                                    # 1. Получаем данные из схемы DBSchema 1С (SerializedData)
        DBSchema_physical   = fetch_DBSchema(cursor)                                    # 2. Получаем данные из физической схемы БД (sys.tables, sys.columns)
        DBSchema_merged     = merge_DBSchema(DBSchema_parsed, DBSchema_physical)        # 3. Объединяем предыдущие таблицы
        DBNmes              = fetch_DBNmes(cursor)                                      # 4. Получаем данные dbnames из таблицы params
        Config              = fetch_Config(cursor, DBNmes)                              # 5. Получаем данные из таблицы config
        Enums               = process_Enums(DBNmes, Config)                             # 6. Обработка перечислений
        Tables              = process_Tables(DBNmes, Config, DBSchema_merged)           # 7. Обработка данных о таблицах
        Fields              = process_Fields(DBNmes, Config, DBSchema_merged, Tables)   # 8. Формируем данные о полях
        Final               = process_Final(Fields, Tables)                             # 9. Собираем в кучу

    except Exception as e:
        print(f"❌ Произошла ошибка: {str(e)}")
        import traceback

        traceback.print_exc()
    finally:
        # Закрываем соединение в блоке finally, чтобы гарантировать закрытие
        # даже если произошла ошибка.
        if conn:
            print("🔗 Закрываем соединение с базой данных.")
            conn.close()