#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <sstream>
#include <math.h>
#include <limits>
#include <vector>
#include <utility>
#include <algorithm>
#include <time.h> 
#include <map>

#define NEG_INFINITY -1.0*numeric_limits<double>::max() 

using namespace std;


/*Variables propias*/
int movies, users, recommendations;
int **matrixUI;
double **matrixCorr;
double *averages;
double *corrTerms;
double *userACorrs;

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
  ofstream fileStreamOutSR (fOutSR.c_str(), ofstream::out);

  for (int i = 0; i < recommendations; ++i) {
    for (int j = 0; j < users; ++j) {
      fileStreamOutSR << matrixSR[i][j] << " ";
    }
    fileStreamOutSR << endl;
  }

  fileStreamOutSR.close();
}

int *
getVectorRecMovies(int a, int *recUsers) {
  int *moviesToRecToA = new int[recommendations];
  pair<int,int> ratingAndMovie;
  for (int i = 0; i < recommendations; ++i) {
    vector < pair<int,int> > recUserRatings;
    for (int j = 0; j < movies; ++j) {
      ratingAndMovie = make_pair(matrixUI[j][recUsers[i]], j);
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
        if (matrixUI[movieToRec][a] == 0) {
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

  return moviesToRecToA;
}

int * 
getVectorSR(vector< pair<double, int> > corrValuesA) {
  pair <int, int> ratingAndMovie;
  sort(corrValuesA.rbegin(), corrValuesA.rend()); //Ordena de mayor a menor
                                                  //según el coeficiente de
                                                  //correlación.
  int *recUsers = new int[recommendations];
  for (int i = 0; i < recommendations; ++i) {
    //Este arreglo contiene los usuarios que presentaron 
    //mayor coeficiente de correlación respecto al usuario a.
    recUsers[i] = corrValuesA[i].second;
  }

  return recUsers;
}

void
calculateSRAndRecMovMat() {
  int a;
  pair <double, int> corrAndUser;

  //Para imprimir en el archivo de salida de las películas más recomendadas.
  string fOutRecMov (matrixRecMovsFile);
  ofstream fileStreamOutRecMov (fOutRecMov.c_str(), ofstream::out);

  for (int i = 0; i < users; ++i) {
    vector <pair <double, int> > corrsVector;
    a = i;  //Índice del usuario a.
    for (int j = 0; j < users; ++j) {
      corrAndUser = make_pair(matrixCorr[j][a], j);
      corrsVector.push_back(corrAndUser);
    }
    vectorSR = getVectorSR(corrsVector);
    vectorRecMovies = getVectorRecMovies(a, vectorSR);
    
    
    for (int k = 0; k < recommendations; ++k) {
      //Se ordenan los usuarios más similares al usuario a en su columna.
      matrixSR[k][a] = vectorSR[k];
    }
    /*
    for (int l = 0; l < recommendations; ++l) {
      //Se ordenan las películas a recomendar a ese usuario por filas para facilitar su lectura.
      matrixRecMovies[a][l] = vectorRecMovies[l];
    }
    */

    for (int k = 0; k < recommendations; ++k) {
      fileStreamOutRecMov << vectorRecMovies[k] << " ";
    }
    fileStreamOutRecMov << endl;
  }
  fileStreamOutRecMov.close();
}

double
calcCorrBtWAU(int a, int u, int *userARatings, int *userURatings) {
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
  
  return corrBtwAandU;
}

void
calculateCorrMat() {
  int a, u;
  double corrAU;
  int *userARatings = new int[movies];
  int *userURatings = new int[movies];
  map<int, int*> usersRatings;

  for (int i = 0; i < users; ++i) {
    for (int j = i+1; j < users; ++j) {
      a = i;  //Índice del usuario a.
      u = j;  //Índice del usuario u.

      if (usersRatings.count(a)) {
        userARatings = usersRatings[a];
      } else {
          for (int k = 0; k < movies; ++k) {
            userARatings[k] = matrixUI[k][a];
          }
          usersRatings[a] = userARatings;
      }

      if (usersRatings.count(u)) {
        userURatings = usersRatings[u];
      } else {
          for (int k = 0; k < movies; ++k) {
            userURatings[k] = matrixUI[k][u];
          }
          usersRatings[u] = userURatings;
      }

      corrAU = calcCorrBtWAU(a, u, userARatings, userURatings);
      matrixCorr[a][u] = corrAU;
      matrixCorr[u][a] = corrAU;
      //printf("Corr (%d, %d) = %f\n", a, u, corrAU);
    } 
  }
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
      while (getline(ssFileStreamMatUI, rating, ' ')) {
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

  //Para optimizar procesamiento de los coeficientes de correlación entre dos usuarios.
  averages = new double[users];
  corrTerms = new double[users];

  for (int i = 0; i < users; ++i) {
    //-1.0 en una posición indica que aún no se han calculado
    //ni el promedio ni los términos para la correlación.
    averages[i] = -1.0;
    corrTerms[i] = -1.0; 
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

//argv 1 - filename_in.
//argv 2 - filename_out_SR.
//argv 3 - filepath_out_RecMovs.
//argv 4 - movies.
//argv 5 - users.
//argv 6 - recommendations.

int
main(int argc, char *argv[]) {

  if (argc != 7) {
    printf("Usage: recommendationSystem [matrixUI_file_name] [matrixSR_file_name] [matrixRecMovs_file_name] ");
    printf("[number_of_movies] [number_of_users] [number_of_recommendations_wanted]\n");
    return 0;
  } 

  if (atoi(argv[6]) > atoi(argv[4])) {
    printf("Movies to recommend for each user should be less than or equals to %d.\n", atoi(argv[4]));
    return 0;
  } 

  //Iniciamos cronómetro.
  clock_t t;
  t = clock();

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

  //Calcular la matriz de correlación.
  calculateCorrMat();

  //Calcular las matrices para los archivos de salida.
  calculateSRAndRecMovMat();

  //Imprimir resultado como matriz.
  printMatrixLocs();

  //Imprimir resultado en el archivo de salida.
  copyToFOut();

  //Paramos el cronómetro.
  t = clock() - t;
  printf("Total execution time: %f s.\n", ((float)t)/CLOCKS_PER_SEC);

  return 0;
  
}

 
