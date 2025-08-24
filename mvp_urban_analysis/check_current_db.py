#!/usr/bin/env python3
"""
Проверка текущего состояния БД
"""
import sqlite3

def check_and_clean():
    conn = sqlite3.connect('urban_analysis_fixed.db')
    cursor = conn.cursor()

    print('=== ТЕКУЩЕЕ СОСТОЯНИЕ БД ===')
    cursor.execute('SELECT id, group_name, group_type FROM object_groups ORDER BY id')
    groups = cursor.fetchall()
    for group in groups:
        print(f'ID:{group[0]} | {group[1]} | {group[2]}')

    print('\n=== УДАЛЕНИЕ ДУБЛИКАТА ===')
    cursor.execute('DELETE FROM object_groups WHERE group_type = "resident_complexes"')
    deleted = cursor.rowcount
    print(f'Удалено: {deleted}')

    cursor.execute('SELECT COUNT(*) FROM object_groups')
    final_count = cursor.fetchone()[0]
    print(f'Итого групп: {final_count}')

    conn.commit()
    conn.close()

if __name__ == "__main__":
    check_and_clean()