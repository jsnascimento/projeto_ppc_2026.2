/**
 * Implementação de Relógios Vetoriais (MPI)
 * -------------------------------------------------------------
 * Compilação: mpicc -o rvet rvet.c
 * Execução:   mpiexec -n 3 ./rvet
 */

#include <stdio.h>
#include <string.h>
#include <mpi.h>

/* Estrutura do relógio vetorial (3 processos). */
typedef struct Clock {
    int p[3];
} Clock;

/*
 * Imprime o estado atual do relógio vetorial.
 * Mostra: qual processo realizou a atualização e o vetor resultante.
 */
void PrintClock(const char *acao, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[Atualizado por P%d] %s -> Clock = (%d,%d,%d)\n",
           pid, acao, clock->p[0], clock->p[1], clock->p[2]);
}

/*
 * Evento interno (sem comunicação).
 * Incrementa o contador local do processo.
 */
void Event(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    
    clock->p[pid]++;
    
    PrintClock("Evento interno", clock);
}

/*
 * Envio de mensagem.
 * 1. Obter o rank.
 * 2. Incrementar clock local (evento de envio).
 * 3. Enviar clock->p via MPI_Send.
 */
void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    
    clock->p[pid]++;
    
    MPI_Send(clock->p, 3, MPI_INT, dest, 0, MPI_COMM_WORLD);
    
    PrintClock("Envio de mensagem", clock);
}

/*
 * Recebimento de mensagem.
 * 1. Obter o rank.
 * 2. Receber vetor remoto via MPI_Recv.
 * 3. Incrementar clock local (evento de recebimento).
 * 4. Fazer fusão (max elemento a elemento) com vetor recebido.
 */
void Receive(int src, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    
    int remote_clock[3];
    MPI_Recv(remote_clock, 3, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    
    clock->p[pid]++;
    
    for (int i = 0; i < 3; i++) {
        if (remote_clock[i] > clock->p[i]) {
            clock->p[i] = remote_clock[i];
        }
    }
    
    PrintClock("Recebimento de mensagem", clock);
}

/*
 * Sequência do Processo 0 baseada no diagrama (eventos a até r)
 */
void process0() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);

    Event(&clock);         // a (1,0,0)
    Send(1, &clock);       // b (2,0,0) - envia para P1 (i)
    Receive(1, &clock);    // c (3,1,0) - recebe de P1 (h)
    Send(2, &clock);       // d (4,1,0) - envia para P2 (m)
    Receive(2, &clock);    // e (5,1,2) - recebe de P2 (l)
    Send(1, &clock);       // f (6,1,2) - envia para P1 (j)
    Event(&clock);         // g (7,1,2)
    Event(&clock);         // n (8,1,2)
    Receive(1, &clock);    // o (9,5,4) - recebe de P1 (t)
    Send(2, &clock);       // p (10,5,4)- envia para P2 (α)
    Receive(2, &clock);    // q (11,5,6)- recebe de P2 (z)
    Send(1, &clock);       // r (12,5,6)- envia para P1 (v)
}

/*
 * Sequência do Processo 1 baseada no diagrama (eventos h até w)
 */
void process1() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
    
    Send(0, &clock);       // h (0,1,0) - envia para P0 (c)
    Receive(0, &clock);    // i (2,2,0) - recebe de P0 (b)
    Receive(0, &clock);    // j (6,3,2) - recebe de P0 (f)
    Receive(2, &clock);    // s (6,4,4) - recebe de P2 (x)
    Send(0, &clock);       // t (6,5,4) - envia para P0 (o)
    Event(&clock);         // u (6,6,4)
    Receive(0, &clock);    // v (12,7,6)- recebe de P0 (r)
    Send(2, &clock);       // w (12,8,6)- envia para P2 (β)
}

/*
 * Sequência do Processo 2 baseada no diagrama (eventos k até β)
 */
void process2() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
    
    Event(&clock);         // k (0,0,1)
    Send(0, &clock);       // l (0,0,2) - envia para P0 (e)
    Receive(0, &clock);    // m (4,1,3) - recebe de P0 (d)
    Send(1, &clock);       // x (4,1,4) - envia para P1 (s)
    Event(&clock);         // y (4,1,5) 
    Send(0, &clock);       // z (4,1,6) - envia para P0 (q)
    Receive(0, &clock);    // α (10,5,7)- recebe de P0 (p)
    Receive(1, &clock);    // β (12,8,8)- recebe de P1 (w)
}

int main(void) {
    int my_rank;

    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    if (my_rank == 0)
        process0();
    else if (my_rank == 1)
        process1();
    else if (my_rank == 2)
        process2();

    MPI_Finalize();
    return 0;
}