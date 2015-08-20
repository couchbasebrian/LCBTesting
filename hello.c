#include <libcouchbase/couchbase.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

static void storage_callback(lcb_t instance, const void *cookie, lcb_storage_t op, 
   lcb_error_t err, const lcb_store_resp_t *resp)
{
  printf("Stored %.*s\n", (int)resp->v.v0.nkey, resp->v.v0.key);
}

static void get_callback(lcb_t instance, const void *cookie, lcb_error_t err, 
   const lcb_get_resp_t *resp)
{
  long time_elapsed_nanos = timer_end(vartime);
  printf("Time taken for get (nanoseconds): %ld\n", time_elapsed_nanos);

  printf("Retrieved key %.*s\n", (int)resp->v.v0.nkey, resp->v.v0.key);
  printf("Value is %.*s\n", (int)resp->v.v0.nbytes, resp->v.v0.bytes);
}

static void touch_callback (lcb_t instance, const void *cookie, lcb_error_t error, const lcb_touch_resp_t *resp)
{
       printf("Callback for touch.  error is  0x%.8X \n", error);
}


static void arithmetic_callback(lcb_t instance, const void *cookie, lcb_error_t error, 
   const lcb_arithmetic_resp_t *resp)
{
       printf("Callback for arithmetic.  Error is 0x%.8X \n", error);

lcb_error_t errorCode;

lcb_touch_cmd_t *touch =  calloc(1, sizeof(*touch));
 
const lcb_touch_cmd_t *cmdlist[] = { touch };
touch->v.v0.key = "counter";
touch->v.v0.nkey = strlen(touch->v.v0.key);
touch->v.v0.exptime = 300; // 5 minutes
errorCode = lcb_touch(instance, NULL, 1, cmdlist);

       printf("errorCode is 0x%.8X \n", errorCode);
}



int main(void)
{

  // struct timespec vartime = timer_start();  // begin a timer called 'vartime'
  // vartime = timer_start();  // begin a timer called 'vartime'

  // initializing
  
  struct lcb_create_st cropts = { 0 };
  cropts.version = 3;
  cropts.v.v3.connstr = "couchbase://localhost/default";
  lcb_error_t err;
  lcb_t instance;
  err = lcb_create(&instance, &cropts);
  if (err != LCB_SUCCESS) {
    printf("Couldn't create instance!\n");
    exit(1);
  }
  
  // connecting
  
  lcb_connect(instance);
  lcb_wait(instance);
  if ( (err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS ) {
    printf("Couldn't bootstrap!\n");
    exit(1);
  }
  
  // installing callbacks
  
  lcb_set_store_callback(instance, storage_callback);
  lcb_set_get_callback(instance, get_callback);
 
  (void)lcb_set_arithmetic_callback(instance, arithmetic_callback); 
  (void)lcb_set_touch_callback(instance, touch_callback); 

  lcb_arithmetic_cmd_t *arithmetic = calloc(1, sizeof(*arithmetic));
  arithmetic->version = 0;
  arithmetic->v.v0.key = "counter";
  arithmetic->v.v0.nkey = strlen(arithmetic->v.v0.key);
  arithmetic->v.v0.initial = 0x666;
  arithmetic->v.v0.create = 1;
  arithmetic->v.v0.delta = 1;
  const lcb_arithmetic_cmd_t* commands[] = { arithmetic };
  lcb_arithmetic(instance, NULL, 1, commands);
  
 
  printf("About to do lcb_wait()...\n");
  lcb_wait(instance); 
  printf("Back from lcb_wait()...\n");

  // GET timing test
  // Choose 0 if you don't want to do it
  int doGetTest = 0;

  if (doGetTest == 1) {
 
  lcb_store_cmd_t scmd = { 0 };
  const lcb_store_cmd_t *scmdlist = &scmd;
  scmd.v.v0.key = "Hello";
  scmd.v.v0.nkey = 5;
  scmd.v.v0.bytes = "Couchbase";
  scmd.v.v0.nbytes = 9;
  scmd.v.v0.operation = LCB_SET;
  err = lcb_store(instance, NULL, 1, &scmdlist);
  if (err != LCB_SUCCESS) {
    printf("Couldn't schedule storage operation!\n");
    exit(1);
  }
  lcb_wait(instance); //storage_callback is invoked here
  
  lcb_get_cmd_t gcmd = { 0 };
  const lcb_get_cmd_t *gcmdlist = &gcmd;
  gcmd.v.v0.key = "Hello";
  gcmd.v.v0.nkey = 5;
  err = lcb_get(instance, NULL, 1, &gcmdlist);
  if (err != LCB_SUCCESS) {
    printf("Couldn't schedule get operation!\n");
    exit(1);
  }

  vartime = timer_start();  // begin a timer called 'vartime'
  lcb_wait(instance); // get_callback is invoked here

  }

  printf("Almost done.  Calling lcb_destroy()\n");
  lcb_destroy(instance);

  printf("Goodbye\n");

  return 0;
}
