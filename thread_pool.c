#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

enum task_status {
	TASK_STATUS_CREATED = 1,
	TASK_STATUS_QUEUED,
	TASK_STATUS_IS_RUNNING,
	TASK_STATUS_FINISHED,
	TASK_STATUS_JOINED
};

struct thread_task {
	thread_task_f function;
	void *arg;
	void *result;
	bool detached;
	enum task_status status; 
	pthread_mutex_t *task_lock; 
	pthread_cond_t waiting_task;
	struct thread_task *prev_task;
	struct thread_task *next_task;
};

enum thread_status {
	THREAD_WORKING = 1,
	THREAD_WAITING
};

struct thread {
	pthread_t thread;
	enum thread_status status;
};

struct thread_pool {
	struct thread *threads;
	int thread_count;
	int max_thread_count;
	int task_count;
	bool in_process;
	pthread_mutex_t *pool_lock; 
	pthread_cond_t waiting_tasks;
	struct thread_task *first_in;
	struct thread_task *last_in;
};

struct worker_arg {
	struct thread_pool *pool;
	struct thread *thread_ptr;
};

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
	if (max_thread_count > TPOOL_MAX_THREADS || max_thread_count <= 0) {
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	
	(*pool) = calloc(1, sizeof(struct thread_pool));
	(*pool)->max_thread_count = max_thread_count;
	(*pool)->in_process = true;
	(*pool)->threads = calloc(max_thread_count, sizeof(pthread_t));
	(*pool)->pool_lock = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init((*pool)->pool_lock, NULL);
	pthread_cond_init(&(*pool)->waiting_tasks, NULL);
	
	return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
	return pool->thread_count;
}

int thread_pool_delete(struct thread_pool *pool) {
	pthread_mutex_lock(pool->pool_lock);
	pool->in_process = false;
	pthread_cond_signal(&pool->waiting_tasks);
	pthread_mutex_unlock(pool->pool_lock);

	struct thread_task *task = pool->first_in;
	while (task) {
		if (task->status != TASK_STATUS_JOINED) 
			return TPOOL_ERR_HAS_TASKS;
		task = task->next_task;
	}

	for (int i = 0; i < pool->thread_count; ++i) {
		pthread_join(pool->threads[i].thread, NULL);
	}
	
	free(pool->threads);
	pthread_mutex_destroy(pool->pool_lock);
	pthread_cond_destroy(&pool->waiting_tasks);
	free(pool->pool_lock);
	free(pool);

	return 0;
}

void *worker(void *argm) {	
	struct worker_arg *arg = (struct worker_arg *)argm;
	pthread_mutex_lock(arg->pool->pool_lock);
	
	while (arg->pool->in_process) {
		arg->thread_ptr->status = THREAD_WORKING;

		if (arg->pool->first_in) {
			struct thread_task *curr_task = arg->pool->first_in;

			pthread_mutex_lock(curr_task->task_lock);
			curr_task->status = TASK_STATUS_IS_RUNNING;
			pthread_mutex_unlock(curr_task->task_lock);

			arg->pool->first_in = arg->pool->first_in->next_task;
			--arg->pool->task_count;
			pthread_mutex_unlock(arg->pool->pool_lock);
			curr_task->result = curr_task->function(curr_task->arg); 

			pthread_mutex_lock(curr_task->task_lock);
			if (curr_task->detached) {
				pthread_mutex_unlock(curr_task->task_lock);
				thread_task_delete(curr_task);
				continue;
			}
			curr_task->status = TASK_STATUS_FINISHED;
			pthread_cond_signal(&curr_task->waiting_task);
			pthread_mutex_unlock(curr_task->task_lock);

			continue;		
		} 
		
		while (!arg->pool->first_in && arg->pool->in_process) {
			arg->thread_ptr->status = THREAD_WAITING;
			pthread_cond_wait(&arg->pool->waiting_tasks, \
				arg->pool->pool_lock);
		}
	}
	pthread_mutex_unlock(arg->pool->pool_lock);
	free(arg);
	return NULL;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
	pthread_mutex_lock(pool->pool_lock);

	if (pool->task_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(pool->pool_lock);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	if (pool->last_in) {
		pool->last_in->next_task = task;
		task->prev_task = pool->last_in;
	} else {
		pool->first_in = task;
	}
	pool->last_in = task;
	++pool->task_count;

	pthread_mutex_lock(task->task_lock);
	task->status = TASK_STATUS_QUEUED;
	pthread_mutex_unlock(task->task_lock);

	pthread_cond_signal(&pool->waiting_tasks);
	pthread_mutex_unlock(pool->pool_lock);

	if (task->status < TASK_STATUS_IS_RUNNING) {
		struct thread new_thread;
		struct worker_arg *warg = malloc(sizeof(struct worker_arg));
		warg->pool = pool;
		pthread_create(&new_thread.thread, NULL, worker, warg);
		new_thread.status = THREAD_WAITING;
		pool->threads[pool->thread_count++] = new_thread;
		warg->thread_ptr = &pool->threads[pool->thread_count];
	}

	return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
	(*task) = calloc(1, sizeof(struct thread_task));
	(*task)->function = function;
	(*task)->arg = arg;
	(*task)->detached = false;
	(*task)->status = TASK_STATUS_CREATED;
	(*task)->task_lock = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init((*task)->task_lock, NULL);
	pthread_cond_init(&(*task)->waiting_task, NULL);
	
	return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
	pthread_mutex_lock(task->task_lock);
	bool res = (task->status == TASK_STATUS_FINISHED);
	pthread_mutex_unlock(task->task_lock);

	return res;
}

bool thread_task_is_running(const struct thread_task *task) {
	pthread_mutex_lock(task->task_lock);
	bool res = (task->status == TASK_STATUS_IS_RUNNING);
	pthread_mutex_unlock(task->task_lock);

	return res;
}

int thread_task_join(struct thread_task *task, void **result) {
	pthread_mutex_lock(task->task_lock);
	if (task->status == TASK_STATUS_CREATED) {
		pthread_mutex_unlock(task->task_lock);
		return TPOOL_ERR_TASK_NOT_PUSHED;	
	}
	
	while (task->status != TASK_STATUS_FINISHED) {
		pthread_cond_wait(&task->waiting_task, task->task_lock);
	}
	
	*result = task->result;
	task->status = TASK_STATUS_JOINED; 
	pthread_mutex_unlock(task->task_lock);

	return 0;
}

int thread_task_delete(struct thread_task *task) {
	pthread_mutex_lock(task->task_lock);
	if (task->status > TASK_STATUS_CREATED && 
		task->status != TASK_STATUS_JOINED) {
		pthread_mutex_unlock(task->task_lock);
		return TPOOL_ERR_TASK_IN_POOL;
	}

	pthread_mutex_unlock(task->task_lock);
	pthread_mutex_destroy(task->task_lock);
	pthread_cond_destroy(&task->waiting_task);
	free(task->task_lock);
	free(task);
	
	return 0;
}

int thread_task_detach(struct thread_task *task) {
	pthread_mutex_lock(task->task_lock);
	if (task->status == TASK_STATUS_CREATED) {
		pthread_mutex_unlock(task->task_lock);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (task->status >= TASK_STATUS_FINISHED) {
		pthread_mutex_unlock(task->task_lock);
		thread_task_delete(task);
		return 0;
	}

	task->detached = true;
	pthread_mutex_unlock(task->task_lock);

	return 0;
}