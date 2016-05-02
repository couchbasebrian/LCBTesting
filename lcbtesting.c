#include <libcouchbase/couchbase.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Brian Williams
//
// https://github.com/couchbasebrian/LCBTesting
//
// Created August 25, 2015
// Modified April 26, 2016
// Modified May 2, 2016 -- added error counter
//
// Tested on CentOS release 6.6 (Final) with the following:
//
// $ rpm -qa | grep couch
// libcouchbase2-libevent-2.5.8-1.el6.x86_64
// couchbase-server-3.0.3-1716.x86_64
// libcouchbase2-core-2.5.8-1.el6.x86_64
// libcouchbase2-bin-2.5.8-1.el6.x86_64
// libcouchbase-devel-2.5.8-1.el6.x86_64
//
// gcc version 4.4.7 20120313 (Red Hat 4.4.7-11) (GCC) 
//
// Reference URLs:
// http://docs.couchbase.com/sdk-api/couchbase-c-client-2.5.8/index.html
//
// How to use:
// sudo yum install git
// git clone https://github.com/couchbasebrian/LCBTesting.git
// wget http://packages.couchbase.com/clients/c/couchbase-csdk-setup
// sudo perl couchbase-csdk-setup
// cd LCBTesting
// ./compileAndRun.sh

int GLOBALERRORCOUNT = 0;
int GLOBALERRORTHRESH = 10;

void initGlobalErrorCount() {
  GLOBALERRORCOUNT = 0;
}

void incrementGlobalErrorCount() {
  GLOBALERRORCOUNT++;
}

void incrementMaybe(lcb_error_t anErr) {
  if (LCB_SUCCESS != anErr) {
    incrementGlobalErrorCount();
    printf("ERROR: %s\n", lcb_strerror(NULL, anErr));
  }

  if (GLOBALERRORCOUNT > GLOBALERRORTHRESH) {
    exit(1);
  }

}

void displayGlobalErrorCount() {
  printf("ERROR COUNT: %10d\n", GLOBALERRORCOUNT);
}


struct timespec vartime;

// call this function to start a nanosecond-resolution timer
struct timespec timer_start(){
    struct timespec start_time;
    //clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_time);
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    return start_time;
}

// call this function to end a timer, returning nanoseconds elapsed as a long
long timer_end(struct timespec start_time){
    struct timespec end_time;
    //clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_time);
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
    return diffInNanos;
}

// Callbacks for different functions

static void storage_callback(lcb_t instance, const void *cookie, lcb_storage_t op, lcb_error_t err, const lcb_store_resp_t *resp)
{
    printf("  Callback for storage.  Stored %.*s\n", (int)resp->v.v0.nkey, resp->v.v0.key);
    incrementMaybe(err);
}

static void get_callback(lcb_t instance, const void *cookie, lcb_error_t err, const lcb_get_resp_t *resp)
{
    long time_elapsed_nanos = timer_end(vartime);
    printf("  Callback for get.  Time taken for get (nanoseconds): %ld\n", time_elapsed_nanos);
    printf("  Retrieved key %.*s\n", (int)resp->v.v0.nkey, resp->v.v0.key);
    printf("  Value is %.*s\n", (int)resp->v.v0.nbytes, resp->v.v0.bytes);
    incrementMaybe(err);
}

static void touch_callback (lcb_t instance, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp)
{
    printf("  Callback for touch.  error is  0x%.8X \n", error);
    incrementMaybe(error);
}


static void arithmetic_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_arithmetic_resp_t *resp)
{
    printf("  Callback for arithmetic.  Error is 0x%.8X \n", error);
    incrementMaybe(error);
}


// Given an LCB instance and a key name, increment an arithmetic, and
// wait until it finishes, and if there is an error, display it.
static void blockingArithmeticIncrement(lcb_t instance, char *keyName) {

  int keyLength = strlen(keyName);

  lcb_error_t           arithmeticIncrementErrorCode = 0;
  lcb_arithmetic_cmd_t *arithmeticCommand         = calloc(1, sizeof(*arithmeticCommand));

  arithmeticCommand->version      = 0;		// determine if this is factor 	
  arithmeticCommand->v.v0.key     = keyName;
  arithmeticCommand->v.v0.nkey    = keyLength;
  arithmeticCommand->v.v0.create  = 0;	 	// as opposed to 1
  arithmeticCommand->v.v0.delta   = 1;		// increment by one

  const lcb_arithmetic_cmd_t* arithCommands[] = { arithmeticCommand };

  arithmeticIncrementErrorCode = lcb_arithmetic(instance, NULL, 1, arithCommands);
  if (arithmeticIncrementErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule arithmetic increment operation!\n");
    printf("lcb_arithmetic errorCode is 0x%.8X \n", arithmeticIncrementErrorCode);
    exit(1);
  }

  // printf("Arithmetic CREATE: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  // printf("Back from lcb_wait()...\n");

} // end of blockingArithmeticIncrement()



// Given an LCB instance and a key name, create an arithmetic, and
// wait until it finishes, and if there is an error, display it.
static void blockingArithmeticCreate(lcb_t instance, char *keyName, int expTimeSeconds) {

  int keyLength = strlen(keyName);

  lcb_error_t           arithmeticCreateErrorCode = 0;
  lcb_arithmetic_cmd_t *arithmeticCommand         = calloc(1, sizeof(*arithmeticCommand));

  arithmeticCommand->version      = 0;
  arithmeticCommand->v.v0.key     = keyName;
  arithmeticCommand->v.v0.nkey    = keyLength;
  arithmeticCommand->v.v0.exptime = expTimeSeconds; 
  arithmeticCommand->v.v0.create  = 1;
  arithmeticCommand->v.v0.delta   = 1;
  arithmeticCommand->v.v0.initial = 1234500;	// Docs start off with this value, but then get incremented

  // In the UI after the program runs you see docs with content "1234501"

  const lcb_arithmetic_cmd_t* arithCommands[] = { arithmeticCommand };

  arithmeticCreateErrorCode = lcb_arithmetic(instance, NULL, 1, arithCommands);
  if (arithmeticCreateErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule arithmetic create operation!\n");
    printf("lcb_arithmetic errorCode is 0x%.8X \n", arithmeticCreateErrorCode);
    exit(1);
  }

  printf("Arithmetic CREATE: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("Back from lcb_wait()...\n");

} // end of blockingArithmeticCreate()


// Doing a touch will set the expiration time of the item
// http://docs.couchbase.com/sdk-api/couchbase-c-client-2.4.6/group__lcb-touch.html
void blockingTouch(lcb_t instance, int expTimeSeconds, char *keyName) {

  int keyLength = strlen(keyName);

  lcb_error_t      touchErrorCode = 0;
  lcb_touch_cmd_t *touchCommand   = calloc(1, sizeof(*touchCommand));

  touchCommand->v.v0.key     = keyName;
  touchCommand->v.v0.nkey    = keyLength;
  touchCommand->v.v0.exptime = expTimeSeconds; 
  touchCommand->v.v0.lock    = 0; 

  const lcb_touch_cmd_t *touchCommandList[] = { touchCommand };

  touchErrorCode = lcb_touch(instance, NULL, 1, touchCommandList);
  if (touchErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule touch operation!\n");
    printf("lcb_touch errorCode is 0x%.8X \n", touchErrorCode);
    exit(1);
  }

  printf("TOUCH: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("Back from lcb_wait()...\n");

} // end of blockingTouch()


// Main 
int main(int argc, char* argv[])
{
  printf("Welcome %d.\n", argc);

  if (argc == 1) {
    printf("No arguments supplied.  Using default bucket.\n");
  }
  else if (argc == 2) {
    printf("One argument supplied.  Using bucket named %s.\n", argv[1]);
  }
  else {
    printf("I do not know what you want.\n");
    exit(1);
  }

  char conStrArray[1000];
  // char* connectionString = &conStrArray;
  char* connectionString = conStrArray;
  sprintf(connectionString, "couchbase://localhost/%s", argv[1]);;
  printf("The connection string is: %s\n", connectionString);

  // Do this once
  srand(time(NULL));

  // initializing
  struct lcb_create_st cropts = { 0 };
  cropts.version = 3;
  // cropts.v.v3.connstr = "couchbase://localhost/default";
  cropts.v.v3.connstr = connectionString; 
  lcb_error_t create_err;
  lcb_t instance;
  create_err = lcb_create(&instance, &cropts);
  if (create_err != LCB_SUCCESS) {
    printf("Couldn't create instance!\n");
    exit(1);
  }
  
  // connecting
  lcb_connect(instance);
  lcb_wait(instance);
  lcb_error_t bootstrap_status_err;
  if ( (bootstrap_status_err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS ) {
    printf("Couldn't bootstrap!\n");
    exit(1);
  }
  
  // installing callbacks
  lcb_set_store_callback(instance, storage_callback);
  lcb_set_get_callback(instance, get_callback);
  lcb_set_arithmetic_callback(instance, arithmetic_callback); 
  lcb_set_touch_callback(instance, touch_callback); 

  // Main part of the program
  int numTimesToRun = 50000;   // specify how many times to iterate

  // 1 means TRUE
  int useAStaticKey = 1;    // Whether to use exactly the same key or not

  // If you elect NOT to use a static key...
  int useARandomKey = 1;    // specify whether to use a random or sequential key

  char keyNameBuffer[100];

  int i = 0;

  // Default to a static non changing key
  sprintf(keyNameBuffer, "staticKey");

  for (i = 0; i < numTimesToRun; i++) {

    if (useAStaticKey == 0) {
      if (useARandomKey == 0) {
  	// Do NOT use a random key
        sprintf(keyNameBuffer, "nonRandomKey%d", i);
      }
      else {
	// Use a random key, generate a new one for each iteration of the loop
        int randomNumber = rand();
        sprintf(keyNameBuffer, "randomKey%d", randomNumber);
      }
    }
    
    printf("#### Iteration: %5d Key: %10s\n", i, keyNameBuffer);

    // Perform the arithmetic operation
    blockingArithmeticCreate(instance, keyNameBuffer, 60); // Expiration time of 60 seconds

    // Perform the touch operation
    int docExpirationTime = 60;		// 60 seconds 
    blockingTouch(instance, docExpirationTime, keyNameBuffer);

    // Perform the increment of the key
    blockingArithmeticIncrement(instance, keyNameBuffer);

    printf("\n");

    displayGlobalErrorCount();
  } // loop for numTimesToRun

  printf("Almost done.  Calling lcb_destroy()\n");
  lcb_destroy(instance);

  printf("Goodbye\n");

  return 0;
}

// EOF
