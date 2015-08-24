#include <libcouchbase/couchbase.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct timespec vartime;

int touchesWorked = 0;
int touchesFailed = 0;

int notFoundErrorHappened = 0;


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

static void storage_callback(lcb_t instance, const void *cookie, lcb_storage_t op, lcb_error_t error, const lcb_store_resp_t *resp)
{
    printf("  Callback for storage.  Stored %.*s\n", (int)resp->v.v0.nkey, resp->v.v0.key);
    if (error != LCB_SUCCESS) {
        	printf("Error received in storage callback\n");
    }
}

static void get_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_get_resp_t *resp)
{
    printf("  Callback for get.       Error is  0x%.8X nkey is %d nbytes is %d\n", error, 
         resp->v.v0.nkey,
         resp->v.v0.nbytes);
	printf("        Value is %s\n", resp->v.v0.bytes);

    if (error != LCB_SUCCESS) {
        	printf("Error received in get callback\n");
    }
}

static void touch_callback (lcb_t instance, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp)
{
    printf("  Callback for touch.       Error is  0x%.8X \n", error);
    if (error != LCB_SUCCESS) {
	touchesFailed++;
	if (notFoundErrorHappened == 0) {
        	printf("Error in touch callback, but assuming this is because it is a new key.  Continuing.\n");
		notFoundErrorHappened = 1;
	}
	else {
        	printf("Error received in touch callback\n");
	}
    } else {
	touchesWorked++;
    }
}


static void remove_callback (lcb_t instance, const void *cookie, lcb_error_t error, const lcb_remove_resp_t *resp)
{
    printf("  Callback for delete.       Error is  0x%.8X \n", error);
    if (error != LCB_SUCCESS) {
        	printf("Error received in touch callback\n");
    }
}


static void arithmetic_callback(lcb_t instance, const void *cookie, lcb_error_t error, const lcb_arithmetic_resp_t *resp)
{
    lcb_uint64_t returnedValue = 0;
    returnedValue = resp->v.v0.value;
    printf("  Callback for arithmetic.  Error is 0x%.8X    Value is %d\n", error, returnedValue);
    if (error != LCB_SUCCESS) {
        printf("Exiting due to error received in arithmetic callback\n");
        exit(1);
    }
}


static void blockingRemove(lcb_t instance, char*keyName) {

	int keyLength = strlen(keyName);

	lcb_error_t	removeErrorCode = 0;
	lcb_remove_cmd_t  *removeCommand = calloc(1,sizeof(*removeCommand));

	removeCommand->version	 = 0;
	removeCommand->v.v0.key  = keyName;
	removeCommand->v.v0.nkey = keyLength;
	removeCommand->v.v0.cas  = 0;

  	const lcb_remove_cmd_t* removeCommands[] = { removeCommand };

  removeErrorCode = lcb_remove(instance, NULL, 1, removeCommands);
  if (removeErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule remove operation!\n");
    printf("lcb_remove errorCode is 0x%.8X \n", removeErrorCode);
    exit(1);
  }

  printf("REMOVE: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("REMOVE: Back from lcb_wait()...\n");

}


static void blockingGet(lcb_t instance, char*keyName) {

	int keyLength = strlen(keyName);

	lcb_error_t	getErrorCode = 0;
	lcb_get_cmd_t  *getCommand = calloc(1,sizeof(*getCommand));

	getCommand->version	 = 0;
	getCommand->v.v0.key  = keyName;
	getCommand->v.v0.nkey = keyLength;

  	const lcb_get_cmd_t* getCommands[] = { getCommand };

  getErrorCode = lcb_get(instance, NULL, 1, getCommands);
  if (getErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule get operation!\n");
    printf("lcb_get errorCode is 0x%.8X \n", getErrorCode);
    exit(1);
  }

  printf("GET: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("GET: Back from lcb_wait()...\n");

} // blockingGet()



// Given an LCB instance and a key name, increment an arithmetic, and
// wait until it finishes, and if there is an error, display it.
static void blockingArithmeticIncrement2(lcb_t instance, char *keyName) {

  int keyLength = strlen(keyName);

  lcb_error_t           arithmeticIncrementErrorCode = 0;
  lcb_arithmetic_cmd_t *arithmeticCommand         = calloc(1, sizeof(*arithmeticCommand));

  // These are the exact values
  arithmeticCommand->version      = 0;		
  arithmeticCommand->v.v0.key     = keyName;
  arithmeticCommand->v.v0.nkey    = strlen(keyName);
  arithmeticCommand->v.v0.exptime = 0;
  arithmeticCommand->v.v0.create  = 1;	 	
  arithmeticCommand->v.v0.delta   = 1;	 // Unknown since not printed in logs
  arithmeticCommand->v.v0.initial = 1;   // Equal to delta	

  const lcb_arithmetic_cmd_t* arithCommands[] = { arithmeticCommand };

  arithmeticIncrementErrorCode = lcb_arithmetic(instance, NULL, 1, arithCommands);
  if (arithmeticIncrementErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule increment operation!\n");
    printf("lcb_arithmetic errorCode is 0x%.8X \n", arithmeticIncrementErrorCode);
    exit(1);
  }

  printf("INCREMENT: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("INCREMENT: Back from lcb_wait()...\n");

} // end of blockingArithmeticIncrement2()


// Given an LCB instance and a key name, increment an arithmetic, and
// wait until it finishes, and if there is an error, display it.
static void OrigBlockingArithmeticIncrement(lcb_t instance, char *keyName) {

  int keyLength = strlen(keyName);

  lcb_error_t           arithmeticIncrementErrorCode = 0;
  lcb_arithmetic_cmd_t *arithmeticCommand         = calloc(1, sizeof(*arithmeticCommand));

  arithmeticCommand->version      = 0;		// Determine if this is factor 
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

  printf("Arithmetic CREATE: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("Arithmetic CREATE: Back from lcb_wait()...\n");

} // end of blockingArithmeticIncrement()



// Given an LCB instance and a key name, create an arithmetic, and
// wait until it finishes, and if there is an error, display it.
static void OrigrBlockingArithmeticCreate(lcb_t instance, char *keyName, int expTimeSeconds) {

  int keyLength = strlen(keyName);

  lcb_error_t           arithmeticCreateErrorCode = 0;
  lcb_arithmetic_cmd_t *arithmeticCommand         = calloc(1, sizeof(*arithmeticCommand));

  arithmeticCommand->version      = 0;
  arithmeticCommand->v.v0.key     = keyName;
  arithmeticCommand->v.v0.nkey    = keyLength;
  arithmeticCommand->v.v0.exptime = expTimeSeconds; 
  arithmeticCommand->v.v0.create  = 1;
  arithmeticCommand->v.v0.delta   = 1;
  arithmeticCommand->v.v0.initial = 1234500;

  const lcb_arithmetic_cmd_t* arithCommands[] = { arithmeticCommand };

  arithmeticCreateErrorCode = lcb_arithmetic(instance, NULL, 1, arithCommands);
  if (arithmeticCreateErrorCode != LCB_SUCCESS) {
    printf("Couldn't schedule arithmeetic create operation!\n");
    printf("lcb_arithmetic errorCode is 0x%.8X \n", arithmeticCreateErrorCode);
    exit(1);
  }

  printf("Arithmetic CREATE: About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("Arithmetic CREATE: Back from lcb_wait()...\n");

} // end of blockingArithmeticCreate()


// Doing a touch will set the expiration time of the item
// http://docs.couchbase.com/sdk-api/couchbase-c-client-2.4.6/group__lcb-touch.html
void blockingTouch2(lcb_t instance, int expTimeSeconds, char *keyName) {

  int keyLength = strlen(keyName);

  lcb_error_t      touchErrorCode = 0;
  lcb_touch_cmd_t *touchCommand   = calloc(1, sizeof(*touchCommand));

  // These are exactly the same values
  touchCommand->v.v0.key     = keyName;
  touchCommand->v.v0.nkey    = strlen(keyName);
  touchCommand->v.v0.exptime = 4500; 
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
  printf("TOUCH: Back from lcb_wait()...\n");

} // end of blockingTouch2()


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
  printf("TOUCH: Back from lcb_wait()...\n");

} // end of blockingTouch()


// Main 
int main(int argc, char* argv[])
{
  printf("Welcome.\n");

  // Do this once
  srand(time(NULL));

  // initializing
  struct lcb_create_st cropts = { 0 };
  cropts.version = 3;
  cropts.v.v3.connstr = "couchbase://localhost/default";
  // cropts.v.v3.connstr = "couchbase://192.168.38.101/default";
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
  lcb_set_remove_callback(instance, remove_callback); 

  // Main part of the program
  int numTimesToRun = 10;   // specify how many times to iterate
  int useARandomKey = 1;    // specify whether to use a random or sequential key

  char keyNameBuffer[100];

  int i = 0;

  for (i = 0; i < numTimesToRun; i++) {

    if (useARandomKey == 0) {
	// Do NOT use a random key
        sprintf(keyNameBuffer, "NonRandomKey%d", i);
    }
    else {
	// Use a random key, generate a new one for each iteration of the loop
        int randomNumber = rand();
        sprintf(keyNameBuffer, "randomKey%d", randomNumber);
    }

    printf("#### Iteration: %5d Key: %10s\n", i, keyNameBuffer);

    // Create-Increment
    blockingArithmeticIncrement2(instance, keyNameBuffer); 

    // Make sure its there
    blockingGet(instance, keyNameBuffer); 

    // Delete
    blockingRemove(instance, keyNameBuffer); 

    // Sleep 60
    printf("About to sleep...\n");
    sleep(1);
    printf("Done with sleep...\n");
    
    // Touch, should fail
    blockingTouch2(instance, 60, keyNameBuffer);

    // Increment
    blockingArithmeticIncrement2(instance, keyNameBuffer); 
    
    // Touch
    blockingTouch2(instance, 60, keyNameBuffer);

    printf("##### touches that worked: %d touches that failed %d\n", touchesWorked, touchesFailed);

  } // loop for numTimesToRun

  printf("Almost done.  Calling lcb_destroy()\n");
  lcb_destroy(instance);

  printf("Goodbye\n");

  return 0;
}
