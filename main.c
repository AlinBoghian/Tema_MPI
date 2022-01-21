#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
void logged_send(int src,void *buf,int count,MPI_Datatype type,int dest,int tag,MPI_Comm comm){
    MPI_Send(buf,count,type,dest,tag,comm);
    printf("M(%d,%d)\n",src,dest);
}
void read_file_coordinator(char* filename,int* worker_nr,int **workers){
    FILE *file = fopen(filename,"r");

    char line[10];


    if(file == NULL){
        perror("eroare deschidere fisier\n");
        exit(-1);
    }

    fgets(line, 9, file);
    //*worker_nr = atoi(line);
    printf("nr workers: %d\n",*worker_nr);
    fflush(stdout);
    *workers = malloc(*worker_nr * sizeof(int));
    for(int i = 0 ; i < *worker_nr ; i++){
        fgets(line, 9, file);
        //(*workers)[i] = atoi(line);
    } 
}

void Coordinator_entrypoint(int rank,int nr_workers,int* workers,bool comm_error){

    // send each worker this process's rank and the vector of workers  
    // 
    for(int i = 0; i < nr_workers ; i++){
        logged_send(rank,&nr_workers,1,MPI_INT,i+3,0,MPI_COMM_WORLD);
        logged_send(rank,workers,nr_workers,MPI_INT,i+3,0,MPI_COMM_WORLD);
    }


}

void Worker_entrypoint(int rank){
    
    MPI_Status status;
    int nr_workers;
    int *workers;
    int coordinator_rank;

    //receive first message from coordinator in order to find its rank
    MPI_Recv(&nr_workers,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&status);


    coordinator_rank = status.MPI_SOURCE;
    workers = malloc(sizeof(int) * nr_workers);
    MPI_Recv(workers, 1, MPI_INT, MPI_ANY_SOURCE, 0,MPI_COMM_WORLD,&status);
    MPI_Recv(workers,nr_workers,MPI_INT,nr_workers,0,MPI_COMM_WORLD,&status);
    
    
}

void main(int argc,char** argv){
    int rank, nProcesses;
    char* filename = "cluster0.txt";
    MPI_Init(&argc, &argv);
    MPI_Status status;
    MPI_Request request;

    bool comm_error;
    comm_error = (atoi(argv[1]));
    printf("%d",comm_error);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    int worker_nr;
    int *workers;

    if(rank>=0 && rank < 3){
        filename[7] = '0' + rank;
        printf("%s\n",filename);
        fflush(stdout);
        //read_file_coordinator(filename,&worker_nr,&workers);
        //Coordinator_entrypoint(rank,worker_nr,workers,comm_error);
    }
    else{
        //Worker_entrypoint(rank);
    }

}