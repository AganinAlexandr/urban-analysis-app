import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from data_processor import DataProcessor
from text_analyzer import TextAnalyzer
from geocoder import MoscowGeocoder
import nltk

# Загрузка необходимых ресурсов NLTK
print("Загрузка ресурсов NLTK...")
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('averaged_perceptron_tagger')
nltk.download('punkt_tab')
print("Ресурсы NLTK загружены")

def main():
    # Создаем директорию для результатов, если она не существует
    if not os.path.exists('results'):
        os.makedirs('results')
        
    # Загрузка и обработка данных
    print("Загрузка и обработка данных...")
    processor = DataProcessor("parsed")
    processor.process_all_data()
    df = processor.get_dataframe()
    
    # Отладочная информация
    print("\nИнформация о DataFrame:")
    print(f"Количество строк: {len(df)}")
    print("\nКолонки в DataFrame:")
    print(df.columns.tolist())
    print("\nПервые несколько строк:")
    print(df.head())
    
    if len(df) == 0:
        print("\nОшибка: DataFrame пустой. Проверьте структуру JSON файлов и пути к ним.")
        return
        
    # Анализ текста
    print("\nАнализ текста...")
    analyzer = TextAnalyzer()
    
    # Проверяем наличие колонки с текстом отзывов
    if 'review_text' not in df.columns:
        print("\nОшибка: Колонка 'review_text' не найдена в DataFrame")
        print("Доступные колонки:", df.columns.tolist())
        return
        
    # Обучаем Word2Vec на текстах отзывов
    print("\nОбучение Word2Vec на текстах отзывов...")
    texts = df['review_text'].dropna().tolist()
    analyzer.train_word2vec(texts)
    
    # Создание базовых визуализаций
    print("\nСоздание базовых визуализаций...")
    plt.figure(figsize=(12, 6))
    sns.histplot(data=df, x='review_rating', bins=5)
    plt.title('Распределение оценок')
    plt.savefig('diagrams/ratings_distribution.png')
    plt.close()
    
    # Геокодирование адресов
    print("\nГеокодирование адресов...")
    geocoder = MoscowGeocoder(api_key="4a8fda1a-c9ca-4e3c-97da-e7bd2a15621a")
    df = geocoder.process_dataframe(df)
    
    # Создание визуализаций с учетом районов
    print("\nСоздание визуализаций с учетом районов...")
    
    # 1. Количество объектов по районам
    plt.figure(figsize=(15, 8))
    sns.countplot(data=df, x='district')
    plt.title('Количество объектов по районам')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('diagrams/objects_by_district.png')
    plt.close()
    
    # 2. Распределение оценок по районам
    plt.figure(figsize=(15, 8))
    sns.boxplot(data=df, x='district', y='review_rating')
    plt.title('Распределение оценок по районам')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('diagrams/ratings_by_district.png')
    plt.close()
    
    # 3. Тепловая карта средних оценок по районам и группам
    pivot_table = df.pivot_table(
        values='review_rating',
        index='district',
        columns='group',
        aggfunc='mean'
    )
    plt.figure(figsize=(15, 10))
    sns.heatmap(pivot_table, annot=True, cmap='YlOrRd', center=pivot_table.mean().mean())
    plt.title('Средние оценки по районам и группам')
    plt.tight_layout()
    plt.savefig('diagrams/ratings_heatmap.png')
    plt.close()
    
    # Сохранение результатов
    print("\nСохранение результатов...")
    try:
        # Создаем директорию, если она не существует
        os.makedirs('results', exist_ok=True)
        
        # Пробуем сохранить файл
        df.to_csv('results/processed_data.csv', index=False, encoding='utf-8-sig')
        print("Данные успешно сохранены в 'results/processed_data.csv'")
    except PermissionError:
        print("\nОшибка: Невозможно сохранить файл 'processed_data.csv'")
        print("Пожалуйста, закройте файл, если он открыт в другой программе")
        print("Попробуйте сохранить данные в другой файл...")
        
        # Пробуем сохранить в другой файл
        try:
            backup_file = 'results/processed_data_backup.csv'
            df.to_csv(backup_file, index=False, encoding='utf-8-sig')
            print(f"Данные сохранены в резервный файл '{backup_file}'")
        except Exception as e:
            print(f"Ошибка при сохранении в резервный файл: {str(e)}")
    except Exception as e:
        print(f"\nОшибка при сохранении данных: {str(e)}")
    
    # Создание визуализаций
    print("\nСоздание визуализаций...")
    
    # 1. Распределение объектов по группам и источникам
    plt.figure(figsize=(15, 8))
    group_source_counts = pd.crosstab(df['group'], df['source'])
    group_source_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение объектов по группам и источникам')
    plt.xlabel('Группа объекта')
    plt.ylabel('Количество объектов')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Источник')
    plt.tight_layout()
    plt.savefig('diagrams/group_source_distribution.png')
    plt.close()
    
    # 2. Распределение отзывов по группам и источникам
    plt.figure(figsize=(15, 8))
    review_counts = df.groupby(['group', 'source']).size().unstack()
    review_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение отзывов по группам и источникам')
    plt.xlabel('Группа объекта')
    plt.ylabel('Количество отзывов')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Источник')
    plt.tight_layout()
    plt.savefig('diagrams/reviews_by_group_source.png')
    plt.close()
    
    # 3. Заполненность данных
    # Создаем DataFrame с информацией о заполненности
    completeness_data = []
    for source in df['source'].unique():
        source_df = df[df['source'] == source]
        total = len(source_df)
        
        # Процент заполненности для каждого типа данных
        review_rating_complete = (source_df['review_rating'].notna() & (source_df['review_rating'] != 0)).mean() * 100
        review_text_complete = source_df['review_text'].notna().mean() * 100
        answer_text_complete = source_df['answer_text'].notna().mean() * 100
        
        completeness_data.append({
            'source': source,
            'review_rating': review_rating_complete,
            'review_text': review_text_complete,
            'answer_text': answer_text_complete
        })
    
    completeness_df = pd.DataFrame(completeness_data)
    completeness_df.set_index('source', inplace=True)
    
    # Создаем визуализацию
    plt.figure(figsize=(15, 8))
    completeness_df.plot(kind='bar')
    plt.title('Заполненность данных по источникам')
    plt.xlabel('Источник')
    plt.ylabel('Процент заполненности')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Тип данных')
    plt.tight_layout()
    plt.savefig('diagrams/data_completeness.png')
    plt.close()
    
    # 4. Распределение по районам
    district_counts = df['district'].value_counts()
    plt.figure(figsize=(15, 8))
    district_counts.plot(kind='bar')
    plt.title('Распределение объектов по районам')
    plt.xlabel('Район')
    plt.ylabel('Количество объектов')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('diagrams/district_distribution.png')
    plt.close()
    
    # 5. Распределение по группам объектов
    group_counts = df['group'].value_counts()
    plt.figure(figsize=(12, 6))
    group_counts.plot(kind='bar')
    plt.title('Распределение по группам объектов')
    plt.xlabel('Группа объекта')
    plt.ylabel('Количество')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('diagrams/group_distribution.png')
    plt.close()
    
    # 6. Распределение по районам и группам объектов
    district_group_counts = pd.crosstab(df['district'], df['group'])
    plt.figure(figsize=(15, 8))
    district_group_counts.plot(kind='bar', stacked=True)
    plt.title('Распределение групп объектов по районам')
    plt.xlabel('Район')
    plt.ylabel('Количество объектов')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Группа объекта', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig('diagrams/district_group_distribution.png')
    plt.close()
    
    print("\nОбработка завершена!")
    print(f"Результаты сохранены в директории 'results'")
    print(f"Диаграммы сохранены в директории 'diagrams'")
    print(f"Всего обработано объектов: {len(df)}")
    print(f"Уникальных районов: {len(df['district'].unique())}")
    print(f"Уникальных групп объектов: {len(df['group'].unique())}")

if __name__ == "__main__":
    main() 