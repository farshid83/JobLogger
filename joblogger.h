/*
 * joblogger.h
 *
 *  Created on: Aug 25, 2014
 *      Author: Farshid
 */

#ifndef JOBLOGGER_H_
#define JOBLOGGER_H_

#define ull unsigned long long

#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

ull GetBaseTime(string line);

#endif /* JOBLOGGER_H_ */
