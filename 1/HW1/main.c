#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>
#include <signal.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>

// Size stack
#define STACK_SIZE              1024 * 1024
#define handle_error(msg)       do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define next_coroutine          timeE = clock();                                                                \
                                if(is_finished[id] == 0)                                                        \
                                    myfile.time[id] += (double)(timeE - timeS) / CLOCKS_PER_SEC;                \
                                if(swapcontext(&ctx[id], &ctx[id_new]) == -1)                                   \
                                    handle_error("swapcontext_swap()");                                         \
                                timeS = clock();


// number of files
static int numFiles;

clock_t timeS, timeE;

struct file {
    int *size;
    int **arr;
    double *time;
};

// ctx is array of coroutines
static ucontext_t *ctx, ctx_main;

static struct file myfile;

// flag showing that сoroutine[i] finished
static char *is_finished;



void swap(int *a, int *b, int id, int id_new)
{
    int buf = *a;
    *a = *b;
    *b = buf;
    next_coroutine;
}

int partition(int arr[], int low, int high, int id, int id_new)
{
    int pivot = arr[high];
    int i = (low - 1);

    for(int j = low; j <= high - 1; ++j)
    {
        if(arr[j] <= pivot)
        {
            i++;
            swap(&arr[i], &arr[j], id, id_new);
            next_coroutine;
        }

    }

    swap(&arr[i + 1], &arr[high], id, id_new);
    next_coroutine;

    return (i + 1);
}

void quickSort(int arr[], int low, int high, int id, int id_new)
{
    if(low < high)
    {
        int pi = partition(arr, low, high, id, id_new);
        next_coroutine;

        quickSort(arr, low, pi - 1, id, id_new);
        next_coroutine;

        quickSort(arr, pi + 1, high, id, id_new);
        next_coroutine;
    }
}


void mergeArrays(int arr1[], int arr2[], int size1, int size2, int arr3[])
{
    int i = 0, j = 0, k = 0;
    while(i < size1 && j < size2)
    {
        if (arr1[i] < arr2[j])
            arr3[k++] = arr1[i++];
        else
            arr3[k++] = arr2[j++];
    }

    while (i < size1)
        arr3[k++] = arr1[i++];

    while (j < size2)
        arr3[k++] = arr2[j++];
}


// allocate stack to coroutines
static void *allocate_stack_mprot()
{
    void *stack = malloc(STACK_SIZE);
    mprotect(stack, STACK_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC);
    return stack;
}


static void my_coroutine(int id, char *FileName)
{
    timeS = clock();
    int id_new;
    int s;
    int size;
    bool is_all_finished;

    id_new = (id + 1) % numFiles;

    /*    Open file    */
    FILE* fp;
    fp = fopen(FileName, "r");
    if(fp == NULL)
    {
        printf("Could not open %s\n\r", FileName);
        exit(0);
    };
    printf("id %d \n\rid_new %d\n\r", id, id_new);
    next_coroutine;


    /*    Read size file    */
    while(fscanf(fp, "%d", &s) != EOF)
    {
        size++;
        next_coroutine;
    }
    myfile.size[id] = size;


    /*    Allocate arr to data    */
    myfile.arr[id] = malloc(size * sizeof(int));
    if (myfile.arr[id] == NULL)
    {
        printf("Could not allocate memory to array");
        exit(0);
    }
    next_coroutine;


    /*    Wtrite data from file to arr    */
    rewind(fp);
    for(int j = 0; j < myfile.size[id]; ++j)
    {
        fscanf(fp, "%d", &myfile.arr[id][j]);
        next_coroutine;
    }


    /*    Close file    */
    fclose(fp);
    next_coroutine;

    /*    Sort myfile.arr[id]    */
    quickSort(myfile.arr[id], 0, myfile.size[id] - 1, id, id_new);
    next_coroutine;

    is_finished[id] = 1;


    /*
     *  Ждем пока все корутины закончат работу
    */
    do
    {
        is_all_finished = true;
        for(int i = 0; i < numFiles; ++i)
        {
            if (is_finished[i] == 0)
            {
                is_all_finished = false;
                break;
            }
        }
        /*
         * Если все корутины закончились, прыгаем в ctx[0] (main)
         * Если же - нет, то прыгаем дальше.
        */
        if(is_all_finished)
        {
            if(swapcontext(&ctx[id], &ctx_main) == -1)
                handle_error("swapcontext_swap()");
            break;
        }
        next_coroutine;
    }
    while(true);
}




int main(int argc, char *argv[])
{
    clock_t timeS_main = clock();
    clock_t timeE_main;
    int id, id_new;
    double spent_time;

    numFiles = argc - 1;
    /*
     * myfile.size[i] - размер каждого файла
     * myfile.arr[i] - массив для записи данных из файла в i -ой корутине
     *
     * myfile.time[i] - время выполненения каждой корутины храним в time[i]
    */
    myfile.size = (int*) malloc((numFiles - 1) * sizeof(int));
    myfile.arr = (int**) malloc((numFiles - 1) * sizeof(int*));
    myfile.time = (double*) calloc((numFiles - 1), sizeof(double));

    /*
     * ctx[i] - массив контекстов:
     * ctx[0] : указывает на my_coroutine(0)
     * ctx[1] : указывает на my_coroutine(1)
     * ...
     * ctx[numFiles - 1] : указывает на my_coroutine(numFiles - 1)
    */
    ctx = (ucontext_t*) malloc(numFiles * sizeof(ucontext_t));      // (numFiles - 1) - ошибка сегментирования

    /*
     * is_finished[i] - показывает состояние i -ой корутины
     * если is_finished[i] = 1, то значит i -ая корутиня закончилась
    */
    is_finished = (int*) calloc((numFiles - 1), sizeof(char));


    /*
     * если после ./a.out нет аргументов, то выходим.
     * Иначе, едем дальше.
    */
    if (numFiles < 1)
    {
        printf("Enter same fileName\n\r");
        exit(2);
    }
    else
    {
        for(int i = 0; i < numFiles; ++i)
        {
            if (getcontext(&ctx[i]) == -1)
                handle_error("getcontext1");
            ctx[i].uc_stack.ss_sp = allocate_stack_mprot();
            ctx[i].uc_stack.ss_size = STACK_SIZE;
            makecontext(&ctx[i], my_coroutine, 2, i, argv[i + 1]);
        }
    }

    /*
     * Стартуем из ctx_main.
     * Прыгаем в ctx[0] (my_coroutine 0)
    */
    if(swapcontext(&ctx_main, &ctx[0]) == -1)
        handle_error("swapcontext_swap()");


    /*
     * После выполнения всех корутин попадаем в эту точку
     * Выводим время выполнения всех корутин в ms
    */
    printf("\n\r");
    for (int i = 0; i < numFiles; ++i)
    {
        printf("coroutine[%d] = %.3f ms\n\r", i, 1000 * myfile.time[i]);
    }



    // Merge arr
    int length;
    int *arr3;
    int *tmp;
    if(numFiles == 1)
    {
        length = myfile.size[0];
        arr3 = (int*) malloc(length * sizeof(int));
        memcpy(arr3, myfile.arr[0], length);
        FILE* fp = fopen("merge.txt", "w");
        fprintf(fp, "%d ", *arr3);
        fclose(fp);
        free(arr3);
    }
    else
    {
        length = myfile.size[0];
        tmp = (int*) malloc(length * sizeof(int));
        memcpy(tmp, myfile.arr[0], length);
        free(myfile.arr[0]);
        for(int i = 1; i < numFiles; ++i)
        {
                length += myfile.size[i];
                arr3 = (int*) malloc(length * sizeof(int));
                mergeArrays(tmp, myfile.arr[i], length - myfile.size[i], myfile.size[i], arr3);
                free(tmp);
                free(myfile.arr[i]);
                tmp = (int*) malloc(length * sizeof(int));
                memcpy(tmp, arr3, length);
                free(arr3);

        }
        FILE* fp = fopen("merge.txt", "w");
        fprintf(fp, "%d ", *tmp);
        fclose(fp);
        free(tmp);
    }

    /*
     * Время работы всей проги
    */
    timeE_main = clock();
    printf("The program has worked = %.3f ms\n\r", 1000 * (double)(timeE_main - timeS_main) / CLOCKS_PER_SEC);

    return 0;
}
