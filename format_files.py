with open('static/input_sales/23-31.csv', 'r', encoding='utf-8') as file_input:
    with open('static/input_sales/23-31.csv', 'w', encoding='utf-8') as file_output:
        for line in file_input:  # Чтение построчно
            file_output.write(line.replace(',', '.'))