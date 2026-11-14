/**
 * Implementação de Produtor/Consumidor com fila intermediária controlada por 
 * variáveis de condição
 * 
 * Compilação: gcc -o prod_cons prod_cons.c -lpthread
 * Execução:   ./prod_cons
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#define MAX_BUFFER 10
#define NUM_PRODUCERS 3
#define NUM_CONSUMERS 3

// Estrutura do relógio vetorial (3 processos)
typedef struct Clock {
    int p[3];
    int id; // Identificação do relógio 
} Clock;

// Estrutura da fila circular
typedef struct {
    Clock buffer[MAX_BUFFER];
    int front;
    int rear;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} Buffer;

Buffer shared_buffer;

// Função para inicializar o buffer
void init_buffer(Buffer* buffer) {
    buffer->front = 0;
    buffer->rear = -1;
    buffer->count = 0;
    pthread_mutex_init(&buffer->mutex, NULL);
    pthread_cond_init(&buffer->not_full, NULL);
    pthread_cond_init(&buffer->not_empty, NULL);
}

// Função para verificar se o buffer está cheio
int is_full(Buffer* buffer) {
    return buffer->count == MAX_BUFFER;
}

// Função para verificar se o buffer está vazio
int is_empty(Buffer* buffer) {
    return buffer->count == 0;
}

// Função para produzir um relógio vetorial (usada pelos produtores)
Clock produce_clock(int producer_id, int sequence) {
    Clock clock;
    clock.p[0] = producer_id * 10 + sequence; // Valor único para cada produtor
    clock.p[1] = sequence * 2;
    clock.p[2] = sequence * 3;
    clock.id = sequence;
    return clock;
}

// Função de produção (thread produtora)
void* producer(void* arg) {
    int producer_id = *(int*)arg;
    
    printf("Produtor %d iniciado\n", producer_id);
    
    for (int i = 0; i < 5; i++) {  // Cada produtor produz 5 relógios
        Clock clock = produce_clock(producer_id, i);
        
        // Adquirir lock do buffer
        pthread_mutex_lock(&shared_buffer.mutex);
        
        // Esperar se buffer estiver cheio
        while (is_full(&shared_buffer)) {
            printf("Produtor %d esperando: buffer cheio\n", producer_id);
            pthread_cond_wait(&shared_buffer.not_full, &shared_buffer.mutex);
        }
        
        // Adicionar relógio ao buffer
        shared_buffer.rear = (shared_buffer.rear + 1) % MAX_BUFFER;
        shared_buffer.buffer[shared_buffer.rear] = clock;
        shared_buffer.count++;
        
        printf("Produtor %d produziu relógio vetorial: (%d,%d,%d) - ID: %d\n", 
               producer_id, clock.p[0], clock.p[1], clock.p[2], clock.id);
        
        // Notificar consumidores que há novo item
        pthread_cond_signal(&shared_buffer.not_empty);
        pthread_mutex_unlock(&shared_buffer.mutex);
        
        // Tempo de produção (sleep) para simular produção
        if (producer_id == 0) {
            sleep(1);  // Cenário 1: produtor 0 produz a cada 1 segundo
        } else if (producer_id == 1) {
            sleep(1);  // Cenário 1: produtor 1 produz a cada 1 segundo  
        } else {
            sleep(1);  // Cenário 1: produtor 2 produz a cada 1 segundo
        }
    }
    
    printf("Produtor %d finalizado\n", producer_id);
    return NULL;
}

// Função de consumo (thread consumidora)
void* consumer(void* arg) {
    int consumer_id = *(int*)arg;
    
    printf("Consumidor %d iniciado\n", consumer_id);
    
    for (int i = 0; i < 5; i++) {  // Cada consumidor consome 5 relógios
        Clock clock;
        
        // Adquirir lock do buffer
        pthread_mutex_lock(&shared_buffer.mutex);
        
        // Esperar se buffer estiver vazio
        while (is_empty(&shared_buffer)) {
            printf("Consumidor %d esperando: buffer vazio\n", consumer_id);
            pthread_cond_wait(&shared_buffer.not_empty, &shared_buffer.mutex);
        }
        
        // Remover relógio do buffer
        clock = shared_buffer.buffer[shared_buffer.front];
        shared_buffer.front = (shared_buffer.front + 1) % MAX_BUFFER;
        shared_buffer.count--;
        
        printf("Consumidor %d consumiu relógio vetorial: (%d,%d,%d) - ID: %d\n", 
               consumer_id, clock.p[0], clock.p[1], clock.p[2], clock.id);
        
        // Notificar produtores que há espaço disponível
        pthread_cond_signal(&shared_buffer.not_full);
        pthread_mutex_unlock(&shared_buffer.mutex);
        
        // Tempo de consumo (sleep) para simular processamento
        if (consumer_id == 0) {
            sleep(2);  // Cenário 1: consumidor 0 consome a cada 2 segundos
        } else if (consumer_id == 1) {
            sleep(2);  // Cenário 1: consumidor 1 consome a cada 2 segundos
        } else {
            sleep(2);  // Cenário 1: consumidor 2 consome a cada 2 segundos
        }
    }
    
    printf("Consumidor %d finalizado\n", consumer_id);
    return NULL;
}

// Cenário 1: Buffer cheio (produção mais rápida que consumo)
void scenario_1() {
    printf("\n=== CENÁRIO 1: Buffer cheio ===\n");
    printf("Produção: cada produtor produz 1 relógio a cada 1 segundo\n");
    printf("Consumo: cada consumidor consome 1 relógio a cada 2 segundos\n");
    printf("Esperando buffer ficar cheio...\n\n");
    
    // Reiniciar buffer
    init_buffer(&shared_buffer);
    
    pthread_t producers[NUM_PRODUCERS];
    pthread_t consumers[NUM_CONSUMERS];
    
    int producer_ids[NUM_PRODUCERS] = {0, 1, 2};
    int consumer_ids[NUM_CONSUMERS] = {0, 1, 2};
    
    // Criar threads produtoras
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_create(&producers[i], NULL, producer, &producer_ids[i]);
    }
    
    // Criar threads consumidoras
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        pthread_create(&consumers[i], NULL, consumer, &consumer_ids[i]);
    }
    
    // Esperar todas as threads terminarem
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }
    
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        pthread_join(consumers[i], NULL);
    }
}