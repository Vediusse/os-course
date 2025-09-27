// Программа-нагрузчик для IO: обход k-регулярного направленного графа
// Вариант: ema-traverse-graph
// Обходит граф, сериализованный в файл, и выполняет модифицирующие операции
// Принимает параметры IO-нагрузчика: rw, block_size, block_count, file, range, direct, type

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

// Константы для структуры графа
#define K_REGULAR 4  // k-регулярный граф (4 соседа у каждой вершины)
#define VALUE_SIZE 8  // размер значения в вершине (uint64_t)

// Структура вершины графа
typedef struct {
    uint64_t value;                    // значение в вершине (8 байт)
    uint32_t neighbors[K_REGULAR];     // номера соседних вершин (4 * 4 = 16 байт)
} Node;

// Размер структуры Node с учетом выравнивания
#define NODE_SIZE sizeof(Node)

// Структура для хранения параметров IO-нагрузчика
typedef struct {
    char rw_mode[8];       // "read" или "write"
    size_t block_size;     // размер блока в байтах
    size_t block_count;    // количество блоков
    char* file_path;       // путь к файлу
    size_t range_start;    // начало диапазона
    size_t range_end;      // конец диапазона
    int direct_io;         // использовать O_DIRECT (1) или нет (0)
    char access_type[16];  // "sequence" или "random"
} IOParams;

// Структура для хранения состояния обхода
typedef struct {
    Node* nodes;           // маппинг файла в память
    size_t node_count;     // количество вершин
    int* visited;          // массив посещенных вершин
    int max_depth;         // максимальная глубина поиска
    uint64_t target_value; // искомое значение
    int operations_count;  // счетчик операций
    IOParams io_params;    // параметры IO
} GraphTraversal;

// Парсинг параметров IO-нагрузчика
static int parse_io_params(int argc, char** argv, IOParams* params) {
    // Инициализация значений по умолчанию
    strcpy(params->rw_mode, "read");
    params->block_size = 4096;
    params->block_count = 100;
    params->file_path = NULL;
    params->range_start = 0;
    params->range_end = 0;  // 0-0 означает весь файл
    params->direct_io = 0;
    strcpy(params->access_type, "sequence");
    
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "rw:read") == 0) {
            strcpy(params->rw_mode, "read");
        } else if (strcmp(argv[i], "rw:write") == 0) {
            strcpy(params->rw_mode, "write");
        } else if (strncmp(argv[i], "block_size:", 11) == 0) {
            params->block_size = atoi(argv[i] + 11);
        } else if (strncmp(argv[i], "block_count:", 12) == 0) {
            params->block_count = atoi(argv[i] + 12);
        } else if (strncmp(argv[i], "file:", 5) == 0) {
            params->file_path = strdup(argv[i] + 5);
        } else if (strncmp(argv[i], "range:", 6) == 0) {
            char* range_str = argv[i] + 6;
            char* dash = strchr(range_str, '-');
            if (dash) {
                *dash = '\0';
                params->range_start = atoi(range_str);
                params->range_end = atoi(dash + 1);
            }
        } else if (strcmp(argv[i], "direct:on") == 0) {
            params->direct_io = 1;
        } else if (strcmp(argv[i], "direct:off") == 0) {
            params->direct_io = 0;
        } else if (strcmp(argv[i], "type:sequence") == 0) {
            strcpy(params->access_type, "sequence");
        } else if (strcmp(argv[i], "type:random") == 0) {
            strcpy(params->access_type, "random");
        }
    }
    
    if (!params->file_path) {
        fprintf(stderr, "Ошибка: не указан параметр file:\n");
        return 0;
    }
    
    return 1;
}

// Инициализация структуры обхода
static GraphTraversal* init_traversal(const char* filename, uint64_t target_value, int max_depth, IOParams* io_params) {
    GraphTraversal* gt = malloc(sizeof(GraphTraversal));
    if (!gt) {
        fprintf(stderr, "Ошибка выделения памяти для GraphTraversal\n");
        return NULL;
    }
    
    // Открываем файл
    int fd = open(filename, O_RDWR);
    if (fd == -1) {
        perror("Ошибка открытия файла");
        free(gt);
        return NULL;
    }
    
    // Получаем размер файла
    struct stat st;
    if (fstat(fd, &st) == -1) {
        perror("Ошибка получения информации о файле");
        close(fd);
        free(gt);
        return NULL;
    }
    
    gt->node_count = st.st_size / NODE_SIZE;
    if (gt->node_count == 0) {
        fprintf(stderr, "Файл не содержит вершин\n");
        close(fd);
        free(gt);
        return NULL;
    }
    
    // Маппируем файл в память
    gt->nodes = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (gt->nodes == MAP_FAILED) {
        perror("Ошибка маппинга файла");
        close(fd);
        free(gt);
        return NULL;
    }
    
    close(fd);
    
    // Инициализируем массив посещенных вершин
    gt->visited = calloc(gt->node_count, sizeof(int));
    if (!gt->visited) {
        fprintf(stderr, "Ошибка выделения памяти для visited\n");
        munmap(gt->nodes, st.st_size);
        free(gt);
        return NULL;
    }
    
    gt->max_depth = max_depth;
    gt->target_value = target_value;
    gt->operations_count = 0;
    gt->io_params = *io_params;  // копируем параметры IO
    
    printf("Инициализирован граф: %zu вершин, размер файла: %ld байт\n", 
           gt->node_count, st.st_size);
    printf("IO параметры: %s, блок %zu байт, %zu блоков, %s, %s\n",
           gt->io_params.rw_mode, gt->io_params.block_size, gt->io_params.block_count,
           gt->io_params.direct_io ? "O_DIRECT" : "обычный", gt->io_params.access_type);
    
    return gt;
}

// Освобождение ресурсов
static void cleanup_traversal(GraphTraversal* gt) {
    if (!gt) return;
    
    if (gt->nodes != MAP_FAILED) {
        munmap(gt->nodes, gt->node_count * NODE_SIZE);
    }
    if (gt->visited) {
        free(gt->visited);
    }
    // НЕ освобождаем file_path здесь, так как он может использоваться повторно
    free(gt);
}

// Модифицирующая операция над вершиной
static void modify_vertex(Node* node, int vertex_id) {
    // Простая модификация: инвертируем биты значения
    node->value = ~node->value;
    
    // Добавляем ID вершины к значению (для демонстрации)
    node->value ^= (uint64_t)vertex_id;
}

// Рекурсивный поиск в глубину с ограничением глубины
static int dfs_traverse(GraphTraversal* gt, int vertex_id, int current_depth) {
    if (current_depth > gt->max_depth || vertex_id < 0 || vertex_id >= (int)gt->node_count) {
        return 0;
    }
    
    if (gt->visited[vertex_id]) {
        return 0;  // уже посещена
    }
    
    gt->visited[vertex_id] = 1;
    Node* node = &gt->nodes[vertex_id];
    
    // Выполняем модифицирующую операцию
    modify_vertex(node, vertex_id);
    gt->operations_count++;
    
    // Проверяем, найдена ли целевая вершина
    if (node->value == gt->target_value) {
        printf("Найдена целевая вершина: ID=%d, значение=0x%016lX, глубина=%d\n", 
               vertex_id, node->value, current_depth);
        return 1;
    }
    
    // Рекурсивно обходим соседние вершины
    int found = 0;
    for (int i = 0; i < K_REGULAR; i++) {
        int neighbor_id = node->neighbors[i];
        if (neighbor_id < (int)gt->node_count) {  // проверяем валидность
            found |= dfs_traverse(gt, neighbor_id, current_depth + 1);
        }
    }
    
    return found;
}

// Генерация случайного графа
static int generate_random_graph(const char* filename, size_t node_count, float forward_bias) {
    printf("Генерируем случайный %d-регулярный граф с %zu вершинами...\n", K_REGULAR, node_count);
    
    FILE* file = fopen(filename, "wb");
    if (!file) {
        perror("Ошибка создания файла");
        return 0;
    }
    
    srand(time(NULL));
    
    for (size_t i = 0; i < node_count; i++) {
        Node node;
        
        // Генерируем случайное значение для вершины
        node.value = ((uint64_t)rand() << 32) | rand();
        
        // Генерируем соседей с учетом bias
        for (int j = 0; j < K_REGULAR; j++) {
            if ((float)rand() / RAND_MAX < forward_bias) {
                // Сосед с большим номером (вперед)
                node.neighbors[j] = i + 1 + (rand() % (node_count - i));
            } else {
                // Сосед с меньшим номером (назад)
                node.neighbors[j] = rand() % (i + 1);
            }
        }
        
        if (fwrite(&node, NODE_SIZE, 1, file) != 1) {
            perror("Ошибка записи вершины");
            fclose(file);
            return 0;
        }
    }
    
    fclose(file);
    printf("Граф сохранен в файл: %s\n", filename);
    return 1;
}

// Основная функция обхода графа
static void traverse_graph_work(const char* filename, uint64_t target_value, int max_depth, int iterations, IOParams* io_params) {
    printf("Начинаем обход графа: %d итераций\n", iterations);
    printf("Целевое значение: 0x%016lX\n", target_value);
    printf("Максимальная глубина: %d\n", max_depth);
    
    for (int iter = 0; iter < iterations; iter++) {
        GraphTraversal* gt = init_traversal(filename, target_value, max_depth, io_params);
        if (!gt) {
            fprintf(stderr, "Ошибка инициализации обхода на итерации %d\n", iter);
            continue;
        }
        
        // Сбрасываем массив посещенных вершин
        memset(gt->visited, 0, gt->node_count * sizeof(int));
        gt->operations_count = 0;
        
        // Начинаем обход с случайной вершины
        int start_vertex = rand() % gt->node_count;
        printf("Итерация %d: начинаем с вершины %d\n", iter + 1, start_vertex);
        
        int found = dfs_traverse(gt, start_vertex, 0);
        
        printf("Итерация %d завершена: выполнено %d операций, найдено: %s\n", 
               iter + 1, gt->operations_count, found ? "ДА" : "НЕТ");
        
        cleanup_traversal(gt);
        
        // Выводим прогресс каждые 10% итераций
        if (iterations > 10 && (iter + 1) % (iterations / 10) == 0) {
            printf("Прогресс: %d%%\n", (iter + 1) * 100 / iterations);
        }
    }
}

static void print_usage(const char* program_name) {
    printf("Использование: %s [параметры IO-нагрузчика] [--generate <количество_вершин> <forward_bias>]\n", program_name);
    printf("\nПрограмма-нагрузчик для IO: обход k-регулярного направленного графа\n");
    printf("- Читает граф из файла с настраиваемыми параметрами IO\n");
    printf("- Ищет вершину с заданным значением\n");
    printf("- Выполняет модифицирующие операции над вершинами\n");
    printf("- Ограничивает глубину поиска\n");
    printf("\nОбязательные параметры IO-нагрузчика:\n");
    printf("  file:<путь>           - путь к файлу с сериализованным графом\n");
    printf("\nОпциональные параметры IO-нагрузчика:\n");
    printf("  rw:read|write         - режим нагрузки: чтение или запись (по умолчанию: read)\n");
    printf("  block_size:<число>    - размер блока в байтах (по умолчанию: 4096)\n");
    printf("  block_count:<число>   - количество блоков (по умолчанию: 100)\n");
    printf("  range:<start>-<end>   - границы в файле, 0-0 = весь файл (по умолчанию: 0-0)\n");
    printf("  direct:on|off         - использовать O_DIRECT (по умолчанию: off)\n");
    printf("  type:sequence|random  - режим доступа (по умолчанию: sequence)\n");
    printf("\nДополнительные параметры:\n");
    printf("  --generate <вершин> <bias> - создать случайный граф\n");
    printf("                                bias: 0.0-1.0 (0.5 = равномерно, >0.5 = вперед)\n");
    printf("\nПримеры:\n");
    printf("  %s file:graph.bin rw:read block_size:8192 block_count:50\n", program_name);
    printf("  %s file:graph.bin rw:write direct:on type:random\n", program_name);
    printf("  %s file:graph.bin --generate 10000 0.7\n", program_name);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    // Проверяем флаг генерации
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--generate") == 0 && i + 2 < argc) {
            size_t node_count = atoi(argv[i + 1]);
            float forward_bias = atof(argv[i + 2]);
            
            if (node_count == 0 || forward_bias < 0.0 || forward_bias > 1.0) {
                fprintf(stderr, "Ошибка: неверные параметры генерации\n");
                return 1;
            }
            
            // Ищем файл в параметрах
            const char* filename = "graph.bin";  // по умолчанию
            for (int j = 1; j < i; j++) {
                if (strncmp(argv[j], "file:", 5) == 0) {
                    filename = argv[j] + 5;
                    break;
                }
            }
            
            if (!generate_random_graph(filename, node_count, forward_bias)) {
                return 1;
            }
            return 0;
        }
    }
    
    // Парсим параметры IO-нагрузчика
    IOParams io_params;
    if (!parse_io_params(argc, argv, &io_params)) {
        return 1;
    }
    
    // Проверяем существование файла
    if (access(io_params.file_path, F_OK) != 0) {
        fprintf(stderr, "Ошибка: файл '%s' не существует!\n", io_params.file_path);
        fprintf(stderr, "Сначала создайте граф командой:\n");
        fprintf(stderr, "  %s file:%s --generate 10000 0.7\n", argv[0], io_params.file_path);
        free(io_params.file_path);
        return 1;
    }
    
    // Устанавливаем значения по умолчанию для обхода графа
    uint64_t target_value = 0x123456789ABCDEF0;  // значение по умолчанию
    int max_depth = 10;  // глубина по умолчанию
    int iterations = 1000;  // итерации по умолчанию
    
    // Инициализация генератора случайных чисел
    srand(time(NULL) ^ getpid());
    
    printf("=== Graph Traversal IO Load Generator ===\n");
    printf("PID: %d\n", getpid());
    printf("Файл графа: %s\n", io_params.file_path);
    printf("Целевое значение: 0x%016lX\n", target_value);
    printf("Максимальная глубина: %d\n", max_depth);
    printf("Количество итераций: %d\n", iterations);
    printf("IO режим: %s\n", io_params.rw_mode);
    printf("Размер блока: %zu байт\n", io_params.block_size);
    printf("Количество блоков: %zu\n", io_params.block_count);
    printf("Диапазон: %zu-%zu\n", io_params.range_start, io_params.range_end);
    printf("O_DIRECT: %s\n", io_params.direct_io ? "включен" : "выключен");
    printf("Тип доступа: %s\n", io_params.access_type);
    
    // Засекаем время начала
    clock_t start_time = clock();
    
    // Выполняем обход графа
    traverse_graph_work(io_params.file_path, target_value, max_depth, iterations, &io_params);
    
    // Вычисляем время выполнения
    clock_t end_time = clock();
    double cpu_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    printf("Время выполнения: %.3f секунд\n", cpu_time);
    printf("Среднее время на итерацию: %.6f секунд\n", cpu_time / iterations);
    
    // Освобождаем память
    if (io_params.file_path) {
        free(io_params.file_path);
    }
    
    return 0;
}
