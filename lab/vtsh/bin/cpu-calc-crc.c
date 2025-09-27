
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>


static uint32_t crc32_table[256];
static int crc32_table_initialized = 0;


static void init_crc32_table(void) {
    if (crc32_table_initialized) return;
    uint32_t polynomial = 0xEDB88320; 
    for (int i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ polynomial;
            } else {
                crc >>= 1;
            }
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = 1;
}

static uint32_t crc32(const char* data, size_t len) {
    if (!crc32_table_initialized) {
        init_crc32_table();
    }
    
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        uint8_t byte = (uint8_t)data[i];
        crc = crc32_table[(crc ^ byte) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}


static void generate_random_fragment(char* buffer, size_t size) {
    const char charset[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    size_t charset_size = strlen(charset);
    
    for (size_t i = 0; i < size - 1; i++) {
        buffer[i] = charset[rand() % charset_size];
    }
    buffer[size - 1] = '\0';
}


static void cpu_load_work(int iterations) {
    const size_t fragment_size = 100;  
    const int fragments_per_iteration = 50;  
    const size_t total_text_size = fragment_size * fragments_per_iteration;
    
    char* text_buffer = malloc(total_text_size + 1);
    if (!text_buffer) {
        fprintf(stderr, "Ошибка выделения памяти\n");
        exit(1);
    }
    
    printf("Начинаем CPU нагрузку: %d итераций\n", iterations);
    printf("Размер текста на итерацию: %zu байт\n", total_text_size);
    
    uint32_t total_crc = 0;
    
    for (int i = 0; i < iterations; i++) {
        text_buffer[0] = '\0';
        
        for (int j = 0; j < fragments_per_iteration; j++) {
            char fragment[fragment_size];
            generate_random_fragment(fragment, fragment_size);
            strcat(text_buffer, fragment);
        }
        
        uint32_t crc = crc32(text_buffer, strlen(text_buffer));
        total_crc ^= crc;  // XOR для накопления результата
        
        if (iterations > 10 && (i + 1) % (iterations / 10) == 0) {
            printf("Прогресс: %d%% (итерация %d), CRC: 0x%08X\n", 
                   (i + 1) * 100 / iterations, i + 1, crc);
        }
    }
    
    printf("Завершено! Итоговый XOR CRC: 0x%08X\n", total_crc);
    free(text_buffer);
}

static void print_usage(const char* program_name) {
    printf("Использование: %s <количество_итераций>\n", program_name);
    printf("\nПрограмма-нагрузчик для CPU:\n");
    printf("- Генерирует случайные фрагменты текста\n");
    printf("- Конкатенирует их в один текст\n");
    printf("- Вычисляет CRC32 контрольную сумму\n");
    printf("- Повторяет заданное количество раз\n");
    printf("\nПример: %s 100000\n", program_name);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    int iterations = atoi(argv[1]);
    if (iterations <= 0) {
        fprintf(stderr, "Ошибка: количество итераций должно быть положительным числом\n");
        return 1;
    }
    
    srand(time(NULL) ^ getpid());
    
    init_crc32_table();
    
    printf("=== CPU Load Generator: CRC Calculator ===\n");
    printf("PID: %d\n", getpid());
    printf("Количество итераций: %d\n", iterations);
    
    clock_t start_time = clock();
    
    cpu_load_work(iterations);
    
    clock_t end_time = clock();
    double cpu_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    printf("Время выполнения: %.3f секунд\n", cpu_time);
    printf("Среднее время на итерацию: %.6f секунд\n", cpu_time / iterations);
    
    return 0;
}
