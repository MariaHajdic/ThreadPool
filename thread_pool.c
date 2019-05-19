#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <time.h>

#define CHECK(x) do { if (x) exit(1); } while (0)

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
	CHECK(pthread_mutex_init((*pool)->pool_lock, NULL));
	CHECK(pthread_cond_init(&(*pool)->waiting_tasks, NULL));

	return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
	return pool->thread_count;
}

int thread_pool_delete(struct thread_pool *pool) {
	CHECK(pthread_mutex_lock(pool->pool_lock));
	if (pool->task_count) {
		CHECK(pthread_mutex_unlock(pool->pool_lock));
		return TPOOL_ERR_HAS_TASKS;
	}

	pool->in_process = false;
	CHECK(pthread_cond_signal(&pool->waiting_tasks));
	CHECK(pthread_mutex_unlock(pool->pool_lock));
	for (int i = 0; i < pool->thread_count; ++i) {
		CHECK(pthread_join(pool->threads[i].thread, NULL));
	}
	
	free(pool->threads);
	CHECK(pthread_mutex_destroy(pool->pool_lock));
	CHECK(pthread_cond_destroy(&pool->waiting_tasks));
	free(pool->pool_lock);
	free(pool);

	return 0;
}

void *worker(void *argm) {	
	struct worker_arg *arg = (struct worker_arg *)argm;
	CHECK(pthread_mutex_lock(arg->pool->pool_lock));
	
	while (arg->pool->in_process) {
		arg->thread_ptr->status = THREAD_WORKING;

		if (arg->pool->first_in) {
			struct thread_task *curr_task = arg->pool->first_in;
			CHECK(pthread_mutex_lock(curr_task->task_lock));
			curr_task->status = TASK_STATUS_IS_RUNNING;
			CHECK(pthread_mutex_unlock(curr_task->task_lock));
			arg->pool->first_in = arg->pool->first_in->next_task;
			if (arg->pool->first_in == NULL) 
				arg->pool->last_in = NULL;
			--arg->pool->task_count;

			CHECK(pthread_mutex_unlock(arg->pool->pool_lock));
			curr_task->result = curr_task->function(curr_task->arg); 
			CHECK(pthread_mutex_lock(arg->pool->pool_lock));

			CHECK(pthread_mutex_lock(curr_task->task_lock));
			if (curr_task->detached) {
				CHECK(pthread_mutex_unlock(curr_task->task_lock));
				thread_task_delete(curr_task);
				continue;
			}
			curr_task->status = TASK_STATUS_FINISHED;
			CHECK(pthread_cond_signal(&curr_task->waiting_task));
			CHECK(pthread_mutex_unlock(curr_task->task_lock));
			continue;		
		} 
		
		while (!arg->pool->first_in && arg->pool->in_process) {
			arg->thread_ptr->status = THREAD_WAITING;
			CHECK(pthread_cond_wait(&arg->pool->waiting_tasks, \
				arg->pool->pool_lock));
		}
	}
	CHECK(pthread_mutex_unlock(arg->pool->pool_lock));
	free(arg);
	return NULL;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
	task->next_task = NULL;
	task->prev_task = NULL;
	
	CHECK(pthread_mutex_lock(pool->pool_lock));
	if (pool->task_count >= TPOOL_MAX_TASKS) {
		CHECK(pthread_mutex_unlock(pool->pool_lock));
		return TPOOL_ERR_TOO_MANY_TASKS;
	}
	task->status = TASK_STATUS_QUEUED;

	if (pool->last_in) {
		pool->last_in->next_task = task;
		task->prev_task = pool->last_in;
	} else {
		pool->first_in = task;
	}
	pool->last_in = task;
	++pool->task_count;

	for (int i = 0; i <= pool->thread_count; ++i) {
		if (pool->threads[i].status == THREAD_WAITING) {
			CHECK(pthread_cond_signal(&pool->waiting_tasks));
			CHECK(pthread_mutex_unlock(pool->pool_lock));
			return 0;
		}
	}
	CHECK(pthread_mutex_unlock(pool->pool_lock));

	if (pool->thread_count < pool->max_thread_count) {
		struct thread* new_thread = &pool->threads[pool->thread_count++];
		struct worker_arg *warg = malloc(sizeof(struct worker_arg));
		warg->pool = pool;
		new_thread->status = THREAD_WAITING;
		warg->thread_ptr = new_thread;
		CHECK(pthread_create(&new_thread->thread, NULL, worker, warg));
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
	CHECK(pthread_mutex_init((*task)->task_lock, NULL));
	CHECK(pthread_cond_init(&(*task)->waiting_task, NULL));
	
	return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
	CHECK(pthread_mutex_lock(task->task_lock));
	bool res = (task->status == TASK_STATUS_FINISHED);
	CHECK(pthread_mutex_unlock(task->task_lock));
	return res;
}

bool thread_task_is_running(const struct thread_task *task) {
	CHECK(pthread_mutex_lock(task->task_lock));
	bool res = (task->status == TASK_STATUS_IS_RUNNING);
	CHECK(pthread_mutex_unlock(task->task_lock));
	return res;
}

int thread_task_join(struct thread_task *task, void **result) {
	CHECK(pthread_mutex_lock(task->task_lock));
	if (task->status == TASK_STATUS_CREATED) {
		CHECK(pthread_mutex_unlock(task->task_lock));
		return TPOOL_ERR_TASK_NOT_PUSHED;	
	}
	
	while (task->status != TASK_STATUS_FINISHED) {
		CHECK(pthread_cond_wait(&task->waiting_task, task->task_lock));
	}
	
	*result = task->result;
	task->status = TASK_STATUS_JOINED; 
	CHECK(pthread_mutex_unlock(task->task_lock));

	return 0;
}

int thread_task_delete(struct thread_task *task) {
	CHECK(pthread_mutex_lock(task->task_lock));
	if (task->status > TASK_STATUS_CREATED && 
		task->status != TASK_STATUS_JOINED) {
		CHECK(pthread_mutex_unlock(task->task_lock));
		return TPOOL_ERR_TASK_IN_POOL;
	}

	CHECK(pthread_mutex_unlock(task->task_lock));
	CHECK(pthread_mutex_destroy(task->task_lock));
	CHECK(pthread_cond_destroy(&task->waiting_task));
	free(task->task_lock);
	free(task);
	task = NULL;
	
	return 0;
}

int thread_task_detach(struct thread_task *task) {
	CHECK(pthread_mutex_lock(task->task_lock));
	if (task->status == TASK_STATUS_CREATED) {
		CHECK(pthread_mutex_unlock(task->task_lock));
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	if (task->status >= TASK_STATUS_FINISHED) {
		CHECK(pthread_mutex_unlock(task->task_lock));
		thread_task_delete(task);
		return 0;
	}

	task->detached = true;
	CHECK(pthread_mutex_unlock(task->task_lock));

	return 0;
}