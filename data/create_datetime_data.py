import csv
from datetime import datetime, timedelta

start_date = datetime(2023, 10, 1)
end_date = datetime(2023, 10, 31)

interval = timedelta(hours=1)

current_date = start_date

with open('dim_data/datetime_dim.csv', 'w', newline='') as csvfile:
    fieldnames = ['datetime_id', 'datetime', 'year', 'month', 'day', 'hour', 'weekday']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    while current_date <= end_date:
        datetime_id = current_date.strftime("%Y%m%d%H")
        year = current_date.year
        month = current_date.month
        day = current_date.day
        hour = current_date.hour
        weekday = current_date.weekday()

        writer.writerow({'datetime_id': datetime_id,
                         'datetime': current_date,
                         'year': year,
                         'month': month,
                         'day': day,
                         'hour': hour,
                         'weekday': weekday})

        current_date += interval

print("CSV file generated successfully!")
