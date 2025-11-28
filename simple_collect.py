
"""
Упрощенный скрипт для принудительного сбора метрик
"""
import sys
import os
import json
from datetime import datetime


sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def collect_metrics_simple():
    """Простой сбор метрик"""
    print("🔄 Принудительный сбор метрик...")
    
    try:

        import psutil
        

        print("📊 Сбор метрик...")
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        print("✅ Метрики собраны:")
        print(f"  - CPU: {cpu_percent}%")
        print(f"  - Memory: {memory.percent}%")
        print(f"  - Disk: {disk.percent}%")
        

        print("💾 Сохранение в базу данных...")
        from shop_bot.data_manager.database import insert_resource_metric, get_latest_resource_metric, get_metrics_series
        
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
            print(f"✅ Метрика сохранена с ID: {metric_id}")
            

            latest = get_latest_resource_metric('local', 'panel')
            if latest:
                print("✅ Метрика найдена в базе:")
                print(f"  - ID: {latest.get('id')}")
                print(f"  - Время: {latest.get('created_at')}")
                print(f"  - CPU: {latest.get('cpu_percent')}%")
                print(f"  - Memory: {latest.get('mem_percent')}%")
                print(f"  - Disk: {latest.get('disk_percent')}%")
                

                print("\n📊 Проверка данных для 1-часового периода...")
                series_1h = get_metrics_series('local', 'panel', since_hours=1, limit=10)
                print(f"Найдено {len(series_1h)} записей за последний час")
                
                if series_1h:
                    print("Последние записи:")
                    for i, record in enumerate(series_1h[-3:], 1):
                        print(f"  {i}. {record.get('created_at')} - CPU:{record.get('cpu_percent')}% MEM:{record.get('mem_percent')}% DISK:{record.get('disk_percent')}%")
                else:
                    print("⚠️  Нет данных за последний час")
                
                return True
            else:
                print("❌ Не удалось найти сохраненную метрику")
                return False
        else:
            print("❌ Не удалось сохранить метрику")
            return False
            
    except ImportError:
        print("❌ psutil не установлен")
        print("Установите: apt install python3-psutil")
        return False
    except Exception as e:
        print(f"❌ Ошибка при сборе метрик: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Основная функция"""
    print("🚀 Принудительный сбор метрик")
    print("=" * 50)
    
    success = collect_metrics_simple()
    
    if success:
        print("\n🎉 Метрики успешно собраны и сохранены!")
        print("\n💡 Теперь проверьте веб-интерфейс:")
        print("  1. Откройте http://localhost:1488/monitor")
        print("  2. Выберите период '1ч'")
        print("  3. Данные должны появиться в графике")
    else:
        print("\n❌ Не удалось собрать метрики")
        print("Установите psutil: apt install python3-psutil")

if __name__ == "__main__":
    main()
