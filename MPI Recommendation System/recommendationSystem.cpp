#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <sstream>
#include <queue>
#include <math.h>
#include <limits>
#include <vector>
#include <utility>
#include <algorithm>
#include <map>
#include "mpi.h"

#define MASTER 0
#define FROM_MASTER 1 //Envío de mensaje desde el master.
#define FROM_WORKER 2 //Envío de mensaje desde el worker.

#define NEG_INFINITY -1.0*numeric_limits<double>::max() 

using namespace std;

/*Variables de MPI*/
int taskId, numTasks, numWorkers, currentWorker = 0; 
MPI_Status status;
MPI_Request request;

/*Variables propias*/
int movies, users, recommendations;
int **matrixUI;
double **matrixCorr;
int combinatorialFactor = 0; //Determinará la cantidad de valores 
                             //de correlación que recibirá el master.
double *userACorrs;

/*Matriz que se recibe por Broadcast en los workers*/
int **matrixUIInW;

/*Variables para almacenamiento temporal en los workers*/
struct Correlation {
 int a; 
 int u; 

 Correlation (int userA, int userU) : a (userA), u (userU){}
};

queue <Correlation> corrToProc;

struct Sort {
 int a; 
 vector <pair <double, int> > corrValuesA; 

 Sort (int userA, vector <pair <double, int> > corrValuesUserA) : a (userA), corrValuesA (corrValuesUserA){}
};

queue <Sort> corrToSort;

/*Vector, matriz y archivos resultado*/
int *vectorSR;
int **matrixSR;
int *vectorRecMovies;
int **matrixRecMovies;
char *matrixSRFile;
char *matrixRecMovsFile;



void
printMatrixLocs() {
  printf("\n");
  printf("Recommendation matrix was output in: %s\n", matrixSRFile);
  printf("Recommended movies matrix was output in: %s\n", matrixRecMovsFile);
  printf("\n");
}

void
copyToFOut () {
  string fOutSR (matrixSRFile);
  string fOutRecMov (matrixRecMovsFile);
  ofstream fileStreamOutSR (fOutSR.c_str(), ofstream::out);
  ofstream fileStreamOutRecMov (fOutRecMov.c_str(), ofstream::out);

  for (int i = 0; i < recommendations; ++i) {
    for (int j = 0; j < users; ++j) {
      fileStreamOutSR << matrixSR[i][j] << " ";
    }
    fileStreamOutSR << endl;
  }

  for (int i = 0; i < users; ++i) {
    for (int j = 0; j < recommendations; ++j) {
      fileStreamOutRecMov << matrixRecMovies[i][j] << " ";
    }
    fileStreamOutRecMov << endl;
  }

  fileStreamOutSR.close();
  fileStreamOutRecMov.close();
}

void
nextWorker() {
  currentWorker++;
  if (currentWorker > numWorkers) {
    currentWorker = 1;
  }
}

void
receiveRecUsersAndMoviesFromWorkers() {
  currentWorker = 0;
  nextWorker();
  int a;
  for (int i = 0; i < users; ++i) {
    MPI_Recv(&a, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status); 
    MPI_Recv(&vectorSR[0], recommendations, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
    for (int j = 0; j < recommendations; ++j) {
      //Se ordenan los usuarios más similares al usuario a en su columna.
      matrixSR[j][a] = vectorSR[j];
    }
    MPI_Recv(&vectorRecMovies[0], recommendations, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
    for (int k = 0; k < recommendations; ++k) {
      //Se ordenan las películas a recomendar a ese usuario por filas para facilitar su lectura.
      matrixRecMovies[a][k] = vectorRecMovies[k];
    }
    nextWorker();
  }
  currentWorker = 0;
}

void 
sortCorrelations() {
  pair <int, int> ratingAndMovie;
  while (!corrToSort.empty()) {
    Sort corrSort = corrToSort.front();
    int a = corrSort.a;
    vector < pair<double, int> > corrValuesA = corrSort.corrValuesA;
    corrToSort.pop();
    sort(corrValuesA.rbegin(), corrValuesA.rend()); //Ordena de mayor a menor
                                                    //según el coeficiente de
                                                    //correlación.
    int *recUsers = new int[recommendations];
    for (int i = 0; i < recommendations; ++i) {
      //Este arreglo contiene los usuarios que presentaron 
      //mayor coeficiente de correlación respecto al usuario a.
      recUsers[i] = corrValuesA[i].second;
    }
    
    int *moviesToRecToA = new int[recommendations];
    for (int i = 0; i < recommendations; ++i) {
      vector < pair<int,int> > recUserRatings;
      for (int j = 0; j < movies; ++j) {
        ratingAndMovie = make_pair(matrixUIInW[j][recUsers[i]], j);
        recUserRatings.push_back(ratingAndMovie);
      }
      sort(recUserRatings.rbegin(), recUserRatings.rend()); //Ordena de mayor a menor
                                                            //según los ratings dados
                                                            //a las películas por el
                                                            //usuario.

      bool continueRecommending = true;
      bool foundMovieToRec = false;
      int uMovieToRecA;
      for (int k = 0; (k < movies) && continueRecommending; ++k) {
        if (recUserRatings[k].first > 0) {
          //Solo se ingresa si el usuario que recomienda tiene 
          //rating asociado a esa película.
          int movieToRec = recUserRatings[k].second;
          if (matrixUIInW[movieToRec][a] == 0) {
            uMovieToRecA = movieToRec;
            foundMovieToRec = true;
            continueRecommending = false;
          }
        } else {
          continueRecommending = false;
        }
      }

      if (!foundMovieToRec) {
        uMovieToRecA = -1;
      } 
      moviesToRecToA[i] = uMovieToRecA;
    }
    MPI_Send(&a, 1, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD);
    MPI_Send(&recUsers[0], recommendations, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD);
    MPI_Send(&moviesToRecToA[0], recommendations, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD);
  }
}

void
receiveCorrColumnsFromMaster() {
  int a; //Índice del usuario base. 
  double *userACorrValues;
  pair <double, int> corrAndUser;
  MPI_Recv(&a, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
  while (a != -1) {
    vector <pair <double, int> > corrsVector;
    userACorrValues = new double[users];
    MPI_Recv(&userACorrValues[0], users, MPI_DOUBLE, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);

    for (int i = 0; i < users; ++i) {
      //Se guarda el coeficiente de correlación acompañado de su usuario asociado.
      corrAndUser = make_pair(userACorrValues[i], i);
      corrsVector.push_back(corrAndUser);
    }

    Sort corrSort = Sort (a, corrsVector);
    corrToSort.push(corrSort);

    //Pedir otro índice de usuario para procesar otra tarea.
    MPI_Recv(&a, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
  }  
}

void
sendCorrColumnsToWorkers() {
  currentWorker = 0;
  nextWorker();
  int a;
  for (int i = 0; i < users; ++i) {
    a = i;  //Índice del usuario a.
    MPI_Send(&a, 1, MPI_INT, currentWorker, FROM_MASTER, MPI_COMM_WORLD);
    for (int j = 0; j < users; ++j) {
        userACorrs[j] = matrixCorr[j][a];
    }
    MPI_Send(&userACorrs[0], users, MPI_DOUBLE, currentWorker, FROM_MASTER, MPI_COMM_WORLD);
    nextWorker();
  }

  int finishCode = -1; //Sirve para indicar a los workers que ya 
                       //se finalizó con el envío de tareas.
  for (int i = 1; i <= numWorkers; ++i) {
    MPI_Send(&finishCode, 1, MPI_INT, i, FROM_MASTER, MPI_COMM_WORLD);
  }
  currentWorker = 0;
}

void
receiveCorrValuesFromWorkers() {
  currentWorker = 0;
  nextWorker();
  int a, u;
  double corrValue;
  for (int i = 0; i < combinatorialFactor; ++i) {
    MPI_Recv(&a, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
    MPI_Recv(&u, 1, MPI_INT, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
    MPI_Recv(&corrValue, 1, MPI_DOUBLE, currentWorker, FROM_WORKER, MPI_COMM_WORLD, &status);
    matrixCorr[a][u] = corrValue;
    matrixCorr[u][a] = corrValue;
    //printf("Corr (%d, %d) = %f\n", a, u, corrValue);
    nextWorker();
  }
  currentWorker = 0;
}

void
processCorrelations() {
  double *averages = new double[users];
  double *corrTerms = new double[users];
  //Se inicializan los vectores que usarán los workers para poder  
  //enviar los coeficientes de correlación entre los usuarios.
  int *userARatings = new int[movies];
  int *userURatings = new int[movies];

  for (int i = 0; i < users; ++i) {
    //-1.0 en una posición indica que aún no se han calculado
    //ni el promedio ni los términos para la correlación.
    averages[i] = -1.0;
    corrTerms[i] = -1.0; 
  }

  map<int, int*> usersRatings;

  while (!corrToProc.empty()) {
    Correlation correlation = corrToProc.front();
    int a = correlation.a;
    int u = correlation.u;

    if (usersRatings.count(a)) {
      userARatings = usersRatings[a];
    } else {
        for (int i = 0; i < movies; ++i) {
          userARatings[i] = matrixUIInW[i][a];
        }
        usersRatings[a] = userARatings;
    }

    if (usersRatings.count(u)) {
      userURatings = usersRatings[u];
    } else {
        for (int i = 0; i < movies; ++i) {
          userURatings[i] = matrixUIInW[i][u];
        }
        usersRatings[u] = userURatings;
    }

    corrToProc.pop();

    //Encontrar ratings promedio del usuario a.
    double aAvg = 0.0;
    if (averages[a] == -1.0) {
      double contRatA = 0.0;
      for (int i = 0; i < movies; ++i) {
        if (userARatings[i] != 0) {
          aAvg += userARatings[i];
          contRatA += 1.0;
        }
      }
      aAvg = aAvg / contRatA;
      averages[a] = aAvg;
    } else {
        aAvg = averages[a];
    }

    //Encontrar ratings promedio del usuario u.
    double uAvg = 0.0;
    if (averages[u] == -1.0) {
      double contRatU = 0.0;
      for (int j = 0; j < movies; ++j) {
        if (userURatings[j] != 0) {
          uAvg += userURatings[j];
          contRatU += 1.0;
        }
      }
      uAvg = uAvg / contRatU;
      averages[u] = uAvg;
    } else {
        uAvg = averages[u];
    }

    //Calcular el primer término del denominador. 
    double denomFirstTerm = 0.0;
    if (corrTerms[a] == -1.0) {
      for (int i = 0; i < movies; ++i) {
        if (userARatings[i] != 0) {
          denomFirstTerm += pow ((userARatings[i] - aAvg), 2);
        }
      }
      denomFirstTerm = sqrt (denomFirstTerm);
      corrTerms[a] = denomFirstTerm; //Se almacena ese término para no tener
                                     //que volverlo a calcular.
    } else {
        denomFirstTerm = corrTerms[a];
    }

    //Calcular el segundo término del denominador.
    double denomSecondTerm = 0.0;
    if (corrTerms[u] == -1.0) {
      for (int j = 0; j < movies; ++j) {
        if (userURatings[j] != 0) {
          denomSecondTerm += pow ((userURatings[j] - uAvg), 2);
        }
      }
      denomSecondTerm = sqrt (denomSecondTerm);
      corrTerms[u] = denomSecondTerm; //Se almacena ese término para no tener
                                      //que volverlo a calcular.
    } else {
        denomSecondTerm = corrTerms[u];
    }

    //Calcular el denominador completo.
    double denominator = denomFirstTerm * denomSecondTerm;

    //Calular el numerador completo.
    double numerator = 0.0;
    for (int i = 0; i < movies; ++i) {
      //Solamente se incluirán aquellas películas que ambos hayan visto.
      if (userARatings[i] != 0 && userURatings[i] != 0) {
        numerator += ((userARatings[i] - aAvg) * (userURatings[i] - uAvg));
      }
    }

    //Calcular el valor de correlación entre ambos usuarios.
    double corrBtwAandU = numerator / denominator;
    MPI_Send(&a, 1, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD);
    MPI_Send(&u, 1, MPI_INT, MASTER, FROM_WORKER, MPI_COMM_WORLD);
    MPI_Send(&corrBtwAandU, 1, MPI_DOUBLE, MASTER, FROM_WORKER, MPI_COMM_WORLD);
  }
}

void
receiveUsersIndexesFromMaster() {
  int a; //Índice del usuario base. 
  int u; //Índice del otro usuario.
  MPI_Recv(&a, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
  while (a != -1) {
    MPI_Recv(&u, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);

    //Correlation correlation = Correlation (a, u, userARatingsInW, userURatingsInW);
    Correlation correlation = Correlation (a, u);
    corrToProc.push(correlation);

    //Pedir otro índice de usuario para procesar otra tarea.
    MPI_Recv(&a, 1, MPI_INT, MASTER, FROM_MASTER, MPI_COMM_WORLD, &status);
  }
}

void
sendUsersIndexesToWorkers() {
  currentWorker = 0;
  nextWorker();
  int a, u;
  for (int i = 0; i < users; ++i) {
    for (int j = i+1; j < users; ++j) {
      a = i;  //Índice del usuario a.
      u = j;  //Índice del usuario u.
      MPI_Send(&a, 1, MPI_INT, currentWorker, FROM_MASTER, MPI_COMM_WORLD);
      MPI_Send(&u, 1, MPI_INT, currentWorker, FROM_MASTER, MPI_COMM_WORLD);
      combinatorialFactor++;
      nextWorker();
    } 
  }

  int finishCode = -1; //Sirve para indicar a los workers que ya se finalizó con el envío de tareas.
  for (int i = 1; i <= numWorkers; ++i) {
    MPI_Send(&finishCode, 1, MPI_INT, i, FROM_MASTER, MPI_COMM_WORLD);
  }
  currentWorker = 0;
}

void
receiveDataFromMaster() {
  MPI_Bcast(&movies, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&users, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&recommendations, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

  matrixUIInW = new int *[movies];
  for (int i = 0; i < movies; ++i) {
    matrixUIInW[i] = new int[users];
  }
  int *linearMatrixUIInW;
  linearMatrixUIInW = new int[movies*users];
  MPI_Bcast(&linearMatrixUIInW[0], movies*users, MPI_INT, MASTER, MPI_COMM_WORLD);
  for (int i = 0; i < movies; ++i) {
    for (int j = 0; j < users; ++j) {
      matrixUIInW[i][j] = linearMatrixUIInW[(users*i)+j];
    }
  }
}

void
sendDataToWorkers() {
  MPI_Bcast(&movies, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&users, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  MPI_Bcast(&recommendations, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

  int *linearMatrixUI = new int[movies*users];
  for (int i = 0; i < movies; ++i) {
    for (int j = 0; j < users; ++j) {
      linearMatrixUI[(users*i)+j] = matrixUI[i][j];
    }
  }
  MPI_Bcast(&linearMatrixUI[0], movies*users, MPI_INT, MASTER, MPI_COMM_WORLD);
}

void
openFiles(char *fileNameMatrixUI) {
  string fNameMatUI (fileNameMatrixUI);
  ifstream fileStreamMatUI (fNameMatUI.c_str());

  int i = 0;  //Índice de la fila (movie).
  int j = -1; //Índice de la columna (user).
  if (fileStreamMatUI.is_open()){
    string movieRow;
    while (getline (fileStreamMatUI, movieRow)) {
      stringstream ssFileStreamMatUI(movieRow);
      string rating;
      while (getline (ssFileStreamMatUI, rating, ' ')) {
        j++;
        matrixUI[i][j] = atoi(rating.c_str());
      }
      j = -1;
      i++;
    }
    fileStreamMatUI.close();
  } else {
      printf("Unable to open %s\n", fileNameMatrixUI); 
  }
}

void
initMatricesAndResponseMat() {
  matrixUI = new int *[movies];
  for (int i = 0; i < movies; ++i) {
    matrixUI[i] = new int[users];
  }

  matrixCorr = new double *[users];
  for (int i = 0; i < users; ++i) {
    matrixCorr[i] = new double[users];
  }

  //Se hace para asegurarse de que en la diagonal queden valores -infinitos.
  for (int j = 0; j < users; ++j) {
    matrixCorr[j][j] = NEG_INFINITY;
  }

  userACorrs = new double[users];

  vectorSR = new int[recommendations];

  matrixSR = new int *[recommendations];
  for (int i = 0; i < recommendations; ++i) {
    matrixSR[i] = new int[users];
  }

  vectorRecMovies = new int[recommendations];

  matrixRecMovies = new int *[users];
  for (int i = 0; i < users; ++i) {
    matrixRecMovies[i] = new int[recommendations];
  }
}

void 
initMPI(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);         
  MPI_Comm_rank(MPI_COMM_WORLD, &taskId);
  //numTasks corresponde al parámetro que acompaña -np X.
  MPI_Comm_size(MPI_COMM_WORLD, &numTasks);
  numWorkers = numTasks-1;  
}

//argv 1 - filename_in.
//argv 2 - filename_out_SR.
//argv 3 - filepath_out_RecMovs.
//argv 4 - movies.
//argv 5 - users.
//argv 6 - recommendations.

int
main(int argc, char *argv[]) {

  initMPI(argc, argv);
  
  if (taskId == MASTER) {
    if (argc != 7) {
      printf("USAGE: recommendationSystem [matrixUI_file_name] [matrixSR_file_name] [matrixRecMovs_file_name] ");
      printf("[number_of_movies] [number_of_users] [number_of_recommendations_wanted]\n");
      MPI_Finalize();
      return 0;
    } 

    if (atoi(argv[6]) > atoi(argv[4])) {
      printf("Movies to recommend for each user should be less than or equals to %d.\n", atoi(argv[4]));
      MPI_Finalize();
      return 0;
    } 

    //Iniciamos cronómetro.
    double startTime = MPI_Wtime();

    matrixSRFile = argv[2];
    matrixRecMovsFile = argv[3];
    movies = atoi(argv[4]);
    users = atoi(argv[5]);
    recommendations = atoi(argv[6]);

    printf("Processing...\n");
    //Inicializar las películas y usuarios de la matrizUI.
    initMatricesAndResponseMat();

    //Abrir el archivo que contiene matrixUI.
    openFiles(argv[1]);

    //Enviar a los workers los datos de movies y users.
    sendDataToWorkers();

    //Enviar las columnas de matrixUI a los workers.
    sendUsersIndexesToWorkers();

    /*En este punto ya los workers empezaron a procesar y a enviar resultados parciales
      referentes a los valores para la matriz de correlación
    */
    
    //Recibir las respuestas parciales desde los workers.
    receiveCorrValuesFromWorkers();

    //Enviar las columnas de correlación por usuarios a los workers.
    sendCorrColumnsToWorkers();

    //Recibir películas recomendadas para cada usuario desde los workers.
    receiveRecUsersAndMoviesFromWorkers(); 

    //Imprimir resultado como matriz.
    printMatrixLocs();

    //Imprimir resultado en el archivo de salida.
    copyToFOut();

    //Paramos el cronómetro.
    double endTime = MPI_Wtime();

    printf("Total execution time: %f s.\n", endTime - startTime);
  }
  else {

    if (argc != 7) {
      MPI_Finalize();
      return 0;
    } 

    if (atoi(argv[6]) > atoi(argv[4])) {
      MPI_Finalize();
      return 0;
    } 

    //Recibir del master los datos de movies y users.
    receiveDataFromMaster();

    //Recibir las columnas de las matrices desde el master.
    receiveUsersIndexesFromMaster();

    //Procesar los registros de la cola de correlaciones para enviárselos al master.
    processCorrelations();

    //Recibir las columnas de correlación por usuarios desde el master.
    receiveCorrColumnsFromMaster();

    //Ordenar los coeficientes de correlación presentes en la cola de sorts para enviárselos al master.
    sortCorrelations();
  }

  MPI_Finalize();
  return 0;
}
