#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <sstream>


using namespace std;


//argv 1 - filename_RecMovs.
//argv 2 - userID.

int
main(int argc, char *argv[]) {

  if (argc != 3) {
    printf("USAGE: recommendedMovies [matrixRecMovs_file_name] [user_ID]\n");
    return 0;
  } 

  char *matrixRecMovsFile = argv[1];
  int user = atoi(argv[2]);

  string fNameMatRecMovs (matrixRecMovsFile);
  ifstream fileStreamMatRecMovs (fNameMatRecMovs.c_str());

  if (fileStreamMatRecMovs.is_open()){
    string recMoviesForUser;
    bool keepReading = true;
    bool hasOutValues = false;
    int linesRead = -1;
    while (getline (fileStreamMatRecMovs, recMoviesForUser) && keepReading) {
      linesRead++;
      if (linesRead == user) {
        stringstream ssFileStreamMatRecMovs(recMoviesForUser);
        string movieID;
        string out = "";
        while (getline (ssFileStreamMatRecMovs, movieID, ' ')) {
          if (movieID != "-1") {
            hasOutValues = true;
            out += movieID + " ";
          } else {
              out += "X ";
          }   
        }
        if (hasOutValues) {
          printf("Most recommended movies for user <%d>:\n", user);
          printf("%s\n", out.c_str());
        } else {
            printf("No recommended movies found for user <%d>.\n", user);
        }
        keepReading = false;
      }
    }
    if (linesRead < user) {
      printf("No recommended movies found for user <%d>.\n", user);
      printf("Maximum userID is: <%d>.\n", linesRead);
    }
    fileStreamMatRecMovs.close();
  } else {
      printf("Unable to open %s\n", matrixRecMovsFile); 
  }

  return 0;
}
