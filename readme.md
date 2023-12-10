Условие:
Имеется файл размером 1 тб, состоящий из строк. 

Задача:
Нужно написать программу, которая сможет отсортировать этот файл на машине, которой
доступно 500мб ОЗУ.

Запуск:
node ExternalSort.js %INPUT_FILE_NAME% %OUTPUT_FILE_NAME% %MAX_MEMORY_USAGE_IN_BYTES%

Пример: 
node ExternalSort.js input.txt output.txt 50
