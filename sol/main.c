#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#define COORD_0 0
#define COORD_1 1
#define COORD_2 2
#define NUM_COORDS 3

#define LOG 1
void print_topology(int **workers,int *workers_size,int rank){

    
    printf("%d -> ",rank);

    for(int i = 0 ; i < 3; i++){
        printf("%d:",i);
        int j = 0;
        for(j = 0 ; j < workers_size[i] - 1; j++){
             printf("%d,",workers[i][j]);
        }
        printf("%d ",workers[i][j]);
    }
    printf("\n");
}
void logm(int src,int dest){
    if (LOG) printf("\nM(%d,%d)\n",src,dest);
}

void logged_send(int src,void *buf,int count,MPI_Datatype type,int dest,int tag){
    MPI_Send(buf,count,type,dest,tag,MPI_COMM_WORLD);
    logm(src,dest);
}


//tehnic logging-ul nu e corect, dar il folosesc doar pe comunicatorul coord unde rank-urile coincid cu cel global

void logged_bcast(void* buff,int count, MPI_Datatype type, int src, MPI_Comm comm,int global_src){

    int myrank;
    MPI_Comm_rank(MPI_COMM_WORLD,&myrank);
    MPI_Bcast(buff,count, type, src, comm);

    if(global_src != myrank)
        logm(global_src,myrank);
}


void read_file_coordinator(char* filename,int* worker_nr,int **workers){
    FILE *file = fopen(filename,"r");

    char line[10];

    if(file == NULL){
        perror("eroare deschidere fisier\n");
        exit(-1);
    }

    fgets(line, 9, file);
    *worker_nr = atoi(line);

    *workers = malloc(*worker_nr * sizeof(int));
    for(int i = 0 ; i < *worker_nr ; i++){
        fgets(line, 9, file);
        (*workers)[i] = atoi(line);
    } 
}
void calculate_sendcdispl_no_root(int comm_size,int vect_size,int **sendcounts,int **displ){
    int per_worker = vect_size / (comm_size - 1);
    *sendcounts = malloc(sizeof(int) * (comm_size - 1));
    *displ = malloc(sizeof(int) * (comm_size - 1));

    int displ_aux = 0;
    (*sendcounts)[0] = 0;
    (*displ)[0] = 0;
    int total = 0;
    for(int i = 1 ; i < comm_size - 1; i++){
        (*sendcounts)[i] = per_worker;
        (*displ)[i] = displ_aux;
        displ_aux += (*sendcounts)[i];
    }

    (*sendcounts)[comm_size - 1] = vect_size - (comm_size - 2) * per_worker;
    (*displ)[comm_size - 1] = displ_aux;
}
void Coordinator_entrypoint(int rank,int my_workers_nr,int* my_workers,MPI_Comm coord_comm,int vector_size,bool comm_error){

    // send each worker this process's rank and the vector of workers  
    int **workers = (int**)malloc(sizeof(int*)*3);
    int workers_size[3];
    MPI_Status status;
    
    workers[rank] = my_workers;
    workers_size[rank] = my_workers_nr;

    if(!comm_error){
        logged_bcast(&workers_size[COORD_0],1,MPI_INT,COORD_0,coord_comm,COORD_0);
        logged_bcast(&workers_size[COORD_1],1,MPI_INT,COORD_1,coord_comm,COORD_1);
        logged_bcast(&workers_size[COORD_2],1,MPI_INT,COORD_2,coord_comm,COORD_2);
    }else{
        switch(rank){
            case COORD_0:
                logged_send(COORD_0,&workers_size[COORD_0],1,MPI_INT,COORD_2,0);
                MPI_Recv(&workers_size[COORD_1],1,MPI_INT,COORD_2,0,MPI_COMM_WORLD,&status);
                MPI_Recv(&workers_size[COORD_2],1,MPI_INT,COORD_2,1,MPI_COMM_WORLD,&status);
                break;
            case COORD_1:
                logged_send(COORD_1,&workers_size[COORD_1],1,MPI_INT,COORD_2,0);
                MPI_Recv(&workers_size[COORD_0],1,MPI_INT,COORD_2,0,MPI_COMM_WORLD,&status);
                MPI_Recv(&workers_size[COORD_2],1,MPI_INT,COORD_2,1,MPI_COMM_WORLD,&status);
                break;
            case COORD_2:
                MPI_Recv(&workers_size[COORD_0],1,MPI_INT,COORD_0,0,MPI_COMM_WORLD,&status);
                MPI_Recv(&workers_size[COORD_1],1,MPI_INT,COORD_1,0,MPI_COMM_WORLD,&status);
                logged_send(COORD_2,&workers_size[COORD_0],1,MPI_INT,COORD_1,0);
                logged_send(COORD_2,&workers_size[COORD_1],1,MPI_INT,COORD_0,0);
                logged_send(COORD_2,&workers_size[COORD_2],1,MPI_INT,COORD_1,1);
                logged_send(COORD_2,&workers_size[COORD_2],1,MPI_INT,COORD_0,1);
                break;
        }
    }

    for(int i = 0; i < 3; i ++){
        if(i != rank){
            workers[i] = malloc(sizeof(int) * workers_size[i]);
        }
        printf("worker size %d\n",workers_size[i]);
    }
    fflush(stdout);
    if(!comm_error){
        logged_bcast(workers[COORD_0],workers_size[COORD_0],MPI_INT,COORD_0,coord_comm,COORD_0);
        logged_bcast(workers[COORD_1],workers_size[COORD_1],MPI_INT,COORD_1,coord_comm,COORD_1);
        logged_bcast(workers[COORD_2],workers_size[COORD_2],MPI_INT,COORD_2,coord_comm,COORD_2);
    }else{
        switch(rank){
            case COORD_0:
                logged_send(COORD_0,workers[COORD_0],workers_size[COORD_0],MPI_INT,COORD_2,0);
                MPI_Recv(workers[COORD_1],workers_size[COORD_1],MPI_INT,COORD_2,0,MPI_COMM_WORLD,&status);
                MPI_Recv(workers[COORD_2],workers_size[COORD_2],MPI_INT,COORD_2,1,MPI_COMM_WORLD,&status);
                break;
            case COORD_1:
                logged_send(COORD_1,workers[COORD_1],workers_size[COORD_1],MPI_INT,COORD_2,0);
                MPI_Recv(workers[COORD_0],workers_size[COORD_0],MPI_INT,COORD_2,0,MPI_COMM_WORLD,&status);
                MPI_Recv(workers[COORD_2],workers_size[COORD_2],MPI_INT,COORD_2,1,MPI_COMM_WORLD,&status);
                break;
            case COORD_2:
                MPI_Recv(workers[COORD_0],workers_size[COORD_0],MPI_INT,COORD_0,0,MPI_COMM_WORLD,&status);
                MPI_Recv(workers[COORD_1],workers_size[COORD_1],MPI_INT,COORD_1,0,MPI_COMM_WORLD,&status);
                logged_send(COORD_2,workers[COORD_0],workers_size[COORD_0],MPI_INT,COORD_1,0);
                logged_send(COORD_2,workers[COORD_1],workers_size[COORD_1],MPI_INT,COORD_0,0);
                logged_send(COORD_2,workers[COORD_2],workers_size[COORD_2],MPI_INT,COORD_1,1);
                logged_send(COORD_2,workers[COORD_2],workers_size[COORD_2],MPI_INT,COORD_0,1);
                break;
        }
    }

    for(int i = 0; i < my_workers_nr ; i++){
        logged_send(rank,&workers_size[COORD_0],1,MPI_INT,my_workers[i],0);
        logged_send(rank,workers[COORD_0],workers_size[COORD_0],MPI_INT,my_workers[i],0);

        logged_send(rank,&workers_size[COORD_1],1,MPI_INT,my_workers[i],0);
        logged_send(rank,workers[COORD_1],workers_size[COORD_1],MPI_INT,my_workers[i],0);
        
        logged_send(rank,&workers_size[COORD_2],1,MPI_INT,my_workers[i],0);
        logged_send(rank,workers[COORD_2],workers_size[COORD_2],MPI_INT,my_workers[i],0);

    }
    print_topology(workers,workers_size,rank);
    
    //PART !!
    MPI_Comm cluster_comm;
    int cluster_rank;
    int cluster_size;
    int * vect;
    MPI_Comm_split(MPI_COMM_WORLD,rank,rank,&cluster_comm);
    MPI_Comm_size(cluster_comm,&cluster_size);
    MPI_Comm_rank(cluster_comm,&cluster_rank);

    int vect_size[3];
    int my_vect_size;
    int *my_vect;
    int *aux_vect;
    if(rank == COORD_0){
        vect = malloc(sizeof(int) * vector_size);
        my_vect = vect;
        for(int i = 0 ; i < vector_size ; i++){
            vect[i] = i;
        }
        
        int total_workers = workers_size[COORD_0] + workers_size[COORD_1] + workers_size[COORD_2]; 
        vect_size[COORD_0] = vector_size / total_workers * workers_size[COORD_0]; 
        my_vect_size = vect_size[COORD_0];
        vect_size[COORD_1] = vector_size / total_workers * workers_size[COORD_1];
        vect_size[COORD_2] = vector_size - (vect_size[COORD_0] + vect_size[COORD_1]);

        int destination = (comm_error) ? COORD_2 : COORD_1;

        logged_send(COORD_0,&vect_size[COORD_1], 1, MPI_INT, destination, 0);
        logged_send(COORD_0,vect + vect_size[COORD_0], vect_size[COORD_1],MPI_INT,destination,0);

        logged_send(COORD_0,&vect_size[COORD_2], 1, MPI_INT, COORD_2, 0);
        logged_send(COORD_0,vect + vect_size[COORD_1] + vect_size[COORD_0],vect_size[COORD_2],MPI_INT,COORD_2,0);
    }else{
        int source = COORD_0;
        if(comm_error){
            if(rank == COORD_2){
                MPI_Recv(&vect_size[COORD_1],1,MPI_INT,COORD_0,0,MPI_COMM_WORLD,&status);
                aux_vect = malloc(sizeof(int) * vect_size[COORD_1]);
                MPI_Recv(aux_vect,vect_size[COORD_1],MPI_INT,COORD_0,0,MPI_COMM_WORLD,&status);

                logged_send(COORD_2,&vect_size[COORD_1],1,MPI_INT,COORD_1,0);
                logged_send(COORD_2,aux_vect,vect_size[COORD_1],MPI_INT,COORD_1,0);
            }else{
                source = COORD_2;
            }
        }
        MPI_Recv(&my_vect_size,1,MPI_INT,source,0,MPI_COMM_WORLD,&status);
        my_vect = malloc(sizeof(int) * my_vect_size);
        MPI_Recv(my_vect,my_vect_size,MPI_INT,source,0,MPI_COMM_WORLD,&status);
    }   


    MPI_Bcast(&my_vect_size,1,MPI_INT,0,cluster_comm);
    if(rank != 0)
        logm(0,rank); 

    int *sendc,*displ;

    calculate_sendcdispl_no_root(cluster_size,my_vect_size,&sendc,&displ);

    MPI_Scatterv(my_vect,sendc,displ,MPI_INT,NULL,0,MPI_INT,0,cluster_comm);
    MPI_Gatherv(MPI_IN_PLACE,0,MPI_INT,my_vect,sendc,displ,MPI_INT,0,cluster_comm);
    
    MPI_Barrier(MPI_COMM_WORLD);
    switch(rank){
        case COORD_0:;
            int source = (comm_error) ? COORD_2 : COORD_1;
            MPI_Recv(vect+vect_size[COORD_0],vect_size[COORD_1],MPI_INT,source,0,MPI_COMM_WORLD,&status);
            MPI_Recv(vect+vect_size[COORD_0]+vect_size[COORD_1],vect_size[COORD_2],MPI_INT,COORD_2,0,MPI_COMM_WORLD,&status);
            break;
        case COORD_1:;
            int destination = (comm_error) ? COORD_2 : COORD_0;
            logged_send(rank,my_vect,my_vect_size,MPI_INT,destination,0);
            break;
        case COORD_2:
            if(comm_error){
                MPI_Recv(aux_vect,vect_size[COORD_1],MPI_INT,COORD_1,0,MPI_COMM_WORLD,&status);
                logged_send(COORD_2,aux_vect,vect_size[COORD_1],MPI_INT,COORD_0,0);
            }
            logged_send(COORD_2,my_vect,my_vect_size,MPI_INT,COORD_0,0);
            break;
    }

    if(rank == COORD_0){
        printf("\nRezultat: ");
        for(int i = 0; i < vector_size;i++){
            printf("%d ",vect[i]);
        }
    }
    printf("\n");
}
void Worker_entrypoint(int rank){
    
    MPI_Status status;
    int workers_size[3];
    int **workers = (int**)malloc(sizeof(int*) * 3);
    int coordinator_rank;

    //receive first message from coordinator in order to find its rank
    MPI_Recv(&workers_size[COORD_0],1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&status);

    coordinator_rank = status.MPI_SOURCE;
    workers[COORD_0] = malloc(sizeof(int) * workers_size[COORD_0]);
    MPI_Recv(workers[COORD_0], workers_size[COORD_0],MPI_INT,coordinator_rank,0,MPI_COMM_WORLD,&status);

    MPI_Recv(&workers_size[COORD_1],1,MPI_INT,coordinator_rank,0,MPI_COMM_WORLD,&status);
    workers[COORD_1] = malloc(sizeof(int) * workers_size[COORD_1]);
    MPI_Recv(workers[COORD_1], workers_size[COORD_1],MPI_INT,coordinator_rank,0,MPI_COMM_WORLD,&status);

    MPI_Recv(&workers_size[COORD_2],1,MPI_INT,coordinator_rank,0,MPI_COMM_WORLD,&status);
    
    workers[COORD_2] = malloc(sizeof(int) * workers_size[COORD_2]);
    MPI_Recv(workers[COORD_2], workers_size[COORD_2],MPI_INT,coordinator_rank,0,MPI_COMM_WORLD,&status);
    
    print_topology(workers,workers_size,rank);

    //PART 2

    MPI_Comm cluster_comm;
    int cluster_rank;
    int cluster_size;
    int vector_size;
    int* vect;
    int vect_cluster_size;

    MPI_Comm_split(MPI_COMM_WORLD,coordinator_rank,rank,&cluster_comm);
    MPI_Comm_rank(cluster_comm,&cluster_rank);
    MPI_Comm_size(cluster_comm,&cluster_size);

    MPI_Bcast(&vect_cluster_size,1,MPI_INT,0,cluster_comm);

    logm(coordinator_rank,rank);
    int *sendcounts,*displ;

    calculate_sendcdispl_no_root(cluster_size,vect_cluster_size,&sendcounts,&displ);

    vect = malloc(sizeof(int) * sendcounts[cluster_rank]);

    MPI_Scatterv(NULL,sendcounts,displ,MPI_INT,vect,sendcounts[cluster_rank],MPI_INT,0,cluster_comm);
    logm(coordinator_rank,rank);
    for(int i = 0 ; i < sendcounts[cluster_rank]; i++){
        vect[i] = vect[i] * 2;
    }
    MPI_Gatherv(vect,sendcounts[cluster_rank],MPI_INT,NULL,sendcounts,displ,MPI_INT,0,cluster_comm);
    logm(rank,coordinator_rank);

    MPI_Barrier(MPI_COMM_WORLD);
   
}

void main(int argc,char** argv){
    int rank, nProcesses;
    char filename[20];
    strcpy(filename,"cluster0.txt");
    MPI_Init(&argc, &argv);
    MPI_Status status;
    MPI_Request request;
    MPI_Comm coord_comm;
    int vector_size = atoi(argv[1]);

    bool comm_error;
    comm_error = (atoi(argv[2]));
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    int worker_nr;
    int *workers;

    if(rank>=0 && rank < 3){
        filename[7] = '0' + rank;
        MPI_Comm_split(MPI_COMM_WORLD,0,rank,&coord_comm);
        read_file_coordinator(filename,&worker_nr,&workers);
        Coordinator_entrypoint(rank,worker_nr,workers,coord_comm,vector_size,comm_error);
    }
    else{
        MPI_Comm_split(MPI_COMM_WORLD,MPI_UNDEFINED,rank,&coord_comm);
        Worker_entrypoint(rank);
    }
    MPI_Finalize();
}