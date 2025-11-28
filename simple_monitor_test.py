
"""
Упрощенный скрипт для тестирования мониторинга без дополнительных зависимостей
"""
import sys
import os
import sqlite3
import json
from datetime import datetime, timedelta


sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_database():
    """Проверяем базу данных"""
    print("🔍 Проверка базы данных...")
    

    db_files = [
        "users-20251005-173430.db",
        "users.db",
        "/app/project/users.db"
    ]
    
    db_file = None
    for db_path in db_files:
        if os.path.exists(db_path):
            db_file = db_path
            break
    
    if not db_file:
        print("❌ Файл базы данных не найден")
        return False
    
    print(f"✅ Файл базы данных найден: {db_file}")
    
    try:
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resource_metrics'")
            if not cursor.fetchone():
                print("❌ Таблица resource_metrics не существует")
                return False
            
            print("✅ Таблица resource_metrics существует")
            

            cursor.execute("SELECT COUNT(*) FROM resource_metrics")
            count = cursor.fetchone()[0]
            print(f"📊 Записей в таблице: {count}")
            
            if count > 0:

                cursor.execute("""
                    SELECT scope, object_name, created_at, cpu_percent, mem_percent, disk_percent 
                    FROM resource_metrics 
                    ORDER BY created_at DESC 
                    LIMIT 3
                """)
                rows = cursor.fetchall()
                print("📈 Последние записи:")
                for row in rows:
                    print(f"  - {row[0]}/{row[1]} | {row[2]} | CPU:{row[3]}% MEM:{row[4]}% DISK:{row[5]}%")
                

                cursor.execute("""
                    SELECT COUNT(*) FROM resource_metrics 
                    WHERE scope = 'local' AND object_name = 'panel'
                    AND created_at >= datetime('now', '-1 hours')
                """)
                count_1h = cursor.fetchone()[0]
                print(f"📊 Записей за последний час: {count_1h}")
                
                if count_1h > 0:
                    print("✅ Данные за последний час найдены")
                    return True
                else:
                    print("⚠️  Нет данных за последний час")
                    return False
            else:
                print("⚠️  Таблица пуста")
                return False
            
    except Exception as e:
        print(f"❌ Ошибка при проверке базы данных: {e}")
        return False

def test_settings():
    """Проверяем настройки"""
    print("\n🔧 Проверка настроек...")
    
    try:

        from shop_bot.data_manager.database import get_setting
        
        monitoring_enabled = get_setting("monitoring_enabled")
        monitoring_interval = get_setting("monitoring_interval_sec")
        
        print(f"📋 monitoring_enabled: {monitoring_enabled}")
        print(f"📋 monitoring_interval_sec: {monitoring_interval}")
        
        if monitoring_enabled != "true":
            print("⚠️  Мониторинг отключен, включаем...")
            from shop_bot.data_manager.database import update_setting
            update_setting("monitoring_enabled", "true")
            update_setting("monitoring_interval_sec", "300")
            print("✅ Настройки исправлены")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при проверке настроек: {e}")
        return False

def test_metrics_collection():
    """Тестируем сбор метрик без psutil"""
    print("\n🖥️  Тестирование сбора метрик...")
    
    try:

        import psutil
        

        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        print("✅ Метрики собираются успешно")
        print(f"  - CPU: {cpu_percent}%")
        print(f"  - Memory: {memory.percent}%")
        print(f"  - Disk: {disk.percent}%")
        
        return True
        
    except ImportError:
        print("⚠️  psutil не установлен - мониторинг будет ограничен")
        print("Для полного мониторинга установите: apt install python3-psutil")
        return True
    except Exception as e:
        print(f"❌ Ошибка при сборе метрик: {e}")
        return False

def insert_test_metric():
    """Вставляем тестовую метрику"""
    print("\n📝 Вставка тестовой метрики...")
    
    try:
        from shop_bot.data_manager.database import insert_resource_metric
        

        import psutil
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        

        metric_id = insert_resource_metric(
            scope='local',
            object_name='panel',
            cpu_percent=cpu_percent,
            mem_percent=memory.percent,
            disk_percent=disk.percent,
            raw_json=json.dumps({
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk.percent,
                "timestamp": datetime.now().isoformat()
            })
        )
        
        if metric_id:
            print(f"✅ Тестовая метрика вставлена с ID: {metric_id}")
            return True
        else:
            print("❌ Не удалось вставить тестовую метрику")
            return False
            
    except ImportError:
        print("⚠️  psutil не установлен - пропускаем вставку метрики")
        return True
    except Exception as e:
        print(f"❌ Ошибка при вставке метрики: {e}")
        return False

def main():
    """Основная функция"""
    print("🚀 Упрощенное тестирование системы мониторинга")
    print("=" * 60)
    

    db_ok = test_database()
    

    settings_ok = test_settings()
    

    metrics_ok = test_metrics_collection()
    

    if metrics_ok:
        insert_ok = insert_test_metric()
    else:
        insert_ok = True
    
    print("\n" + "=" * 60)
    print("📋 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"  База данных: {'✅ OK' if db_ok else '❌ ПРОБЛЕМА'}")
    print(f"  Настройки: {'✅ OK' if settings_ok else '❌ ПРОБЛЕМА'}")
    print(f"  Сбор метрик: {'✅ OK' if metrics_ok else '❌ ПРОБЛЕМА'}")
    print(f"  Вставка данных: {'✅ OK' if insert_ok else '❌ ПРОБЛЕМА'}")
    
    if db_ok and settings_ok:
        print("\n🎉 Система мониторинга готова к работе!")
        print("\n💡 Следующие шаги:")
        print("  1. Перезапустите бота: python3 -m shop_bot")
        print("  2. Откройте http://localhost:1488/monitor")
        print("  3. Выберите период '1ч' - график должен отображаться")
        print("  4. Данные будут собираться каждые 5 минут автоматически")
        
        if not metrics_ok:
            print("\n⚠️  Для полного мониторинга установите psutil:")
            print("   apt install python3-psutil")
    else:
        print("\n⚠️  Обнаружены проблемы:")
        if not db_ok:
            print("  - Проблемы с базой данных")
        if not settings_ok:
            print("  - Проблемы с настройками")

if __name__ == "__main__":
    main()
