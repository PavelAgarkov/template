package service

import (
	"context"
	"fmt"
	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/models"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	"github.com/PavelAgarkov/template/internal/service/command_bus"
	"github.com/PavelAgarkov/template/internal/service/nomenclature"
	"time"

	"github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	scheduler2 "github.com/PavelAgarkov/service-pkg/scheduler"

	"github.com/jackc/pgx/v5"
)

type Core struct {
	config                   config.Config
	commandService           command_bus.ServiceInterface
	nomenclatureTopicService nomenclature.TopicInterface
	nomenclatureConsumer     scheduler2.JobSchedulerInterface
	commandBusConsumer       scheduler2.JobSchedulerInterface
	cron                     *scheduler2.Cron
}

func NewCore(
	ctx context.Context,
	config config.Config,
	commandService command_bus.ServiceInterface,
	nomenclatureTopicService nomenclature.TopicInterface,
	nomenclatureConsumer scheduler2.JobSchedulerInterface,
	commandBusConsumer scheduler2.JobSchedulerInterface,
	cron *scheduler2.Cron,
) *Core {
	core := &Core{
		config:                   config,
		commandService:           commandService,
		nomenclatureTopicService: nomenclatureTopicService,
		nomenclatureConsumer:     nomenclatureConsumer,
		commandBusConsumer:       commandBusConsumer,
		cron:                     cron,
	}
	core.initSchedulers(ctx)
	return core
}

func (core *Core) initSchedulers(ctx context.Context) {
	core.initTopicTaskConsumer()
	core.initCommandBusConsumer()
	core.initCommandBusTasks(ctx)
}

// initTopicTaskConsumer инициализация потребителя задач по топику
func (core *Core) initTopicTaskConsumer() {
	for _, queueNumber := range core.takeListening() {
		err := core.nomenclatureConsumer.Add(
			scheduler2.JobConfiguration{
				Name: fmt.Sprintf("nm_tasks_scheduler_%v", queueNumber),
				Func: func(ctx context.Context) error {
					err := core.dequeueNm(
						ctx,
						core.scheduleTopicTasks,
						pg_model.OfficeID(queueNumber),
						[]string{pg_model.StreamTaskType, pg_model.RecalculateByTtlTask, pg_model.RecalculateByOfficeIDTask, pg_model.CriticalWarming},
						core.config.Application.Core.AllowedTimeToClickhouseQuery,
					)
					if err != nil {
						//logger_wrapper.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
						//	Msg:       "Dequeue NM task",
						//	Args:      officeID,
						//	Component: "Queuer",
						//	Method:    "initNomenclatureTaskConsumer",
						//	Error:     err,
						//})
						return err
					}
					return nil
				},
				Tick:     core.config.Application.Core.ScheduleNmTasksInterval,
				Deadline: time.Minute,
				StopMode: scheduler2.StopGraceful,
			})

		if err != nil {
			panic(err.Error())
		}
	}
}

// initCommandBusConsumer обработка командных задач по офисам
func (core *Core) initCommandBusConsumer() {
	for _, queueNumber := range core.takeListening() {
		err := core.commandBusConsumer.Add(
			scheduler2.JobConfiguration{
				Name: fmt.Sprintf("command_bus_consumer_%v", queueNumber),
				Func: func(ctx context.Context) error {
					err := core.dequeueCommand(ctx, pg_model.OfficeID(queueNumber), core.commandService.Invoke)
					if err != nil {
						logger.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
							Msg:       "Dequeue command",
							Args:      queueNumber,
							Component: "Queuer",
							Method:    "initCommandBusConsumer",
							Error:     err,
						})
						return err
					}
					return nil
				},
				Tick:     core.config.Application.Core.ScheduleCommandsBusInterval,
				Deadline: time.Minute,
				StopMode: scheduler2.StopGraceful,
			})

		if err != nil {
			panic(err.Error())
		}
	}
}

// initCommandBusTasks геренация задач по офисам, которые будут выполняться в фоне и будут обработаны им initCommandBusConsumer
func (core *Core) initCommandBusTasks(ctx context.Context) {
	for _, queueNumber := range core.takeListening() {
		// эта будет срабатывать каждый час
		core.cron.Add(ctx, core.config.Application.Core.TTLCacheRecompilerInterval,
			func(fnCtx context.Context) error {
				logger.WriteDebugLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "Recalculating NM tasks by TTL",
					Args:      queueNumber,
					Component: "Queuer",
					Method:    "initCronTasks",
				})
				jsonOffset, err := command_bus.CreateStartOffset(pg_model.OfficeID(queueNumber))
				if err != nil {
					return fmt.Errorf("[initCronTasks] CreateStartOffset failed: %w", err)
				}
				return core.commandService.CreateCommand(ctx, &pg_model.Command{
					OfficeID:    pg_model.OfficeID(queueNumber),
					Type:        pg_model.RecalculateCommandByTtl,
					Priority:    pg_model.CommandPriorityMedium,
					QueueNumber: int32(core.computeQueueNumber(queueNumber, models.MaxQueueNumber)),
					Payload:     jsonOffset,
				})
			})

		// это тоже будет срабатывать каждый час
		core.cron.Add(ctx, core.config.Application.Core.ScheduleDeleteUnusedNomenclatureFilterInterval,
			func(fnCtx context.Context) error {
				logger.WriteDebugLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "Deleting unused NM from nomenclature filter",
					Args:      queueNumber,
					Component: "Queuer",
					Method:    "initCronTasks",
				})
				return core.commandService.CreateCommand(ctx, &pg_model.Command{
					OfficeID:    pg_model.OfficeID(queueNumber),
					Type:        pg_model.DeleteNmFromNomenclatureFilterScheduler,
					Priority:    pg_model.CommandPriorityMedium,
					QueueNumber: int32(core.computeQueueNumber(queueNumber, models.MaxQueueNumber)),
				})
			})

		core.cron.Add(ctx, core.config.Application.Core.RemoveUnupdatedNomenclatureCacheInterval,
			func(fnCtx context.Context) error {
				logger.WriteDebugLog(ctx, &logger_wrapper.LogEntry{
					Msg:       "Removing unupdated NM from nomenclature cache",
					Args:      queueNumber,
					Component: "Queuer",
					Method:    "initCronTasks",
				})
				return core.commandService.CreateCommand(ctx, &pg_model.Command{
					OfficeID:    pg_model.OfficeID(queueNumber),
					Type:        pg_model.DeleteFromNomenclatureCacheTurnover,
					Priority:    pg_model.CommandPriorityLow,
					QueueNumber: int32(core.computeQueueNumber(queueNumber, models.MaxQueueNumber)),
				})
			})
	}
}

// ОПРЕДЕЛИТЕ СВОИ ИЛИ ОТКАЖИТЕСЬ ОТ ЭТОЙ ФУНКЦИИ
func (core *Core) takeListening() []int64 {
	return []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10000}
}

func (core *Core) scheduleTopicTasks(ctx context.Context, tx pgx.Tx, task *pg_model.Task) error {
	if task == nil {
		return nil
	}

	relationTasks, err := core.nomenclatureTopicService.GetRelationForStreamNomenclatureTask(ctx, tx, task)
	if err != nil {
		return fmt.Errorf("[scheduleNmTasks] GetRelationForStreamNomenclatureTask failed: %w", err)
	}
	if len(relationTasks) == 0 {
		return fmt.Errorf("[scheduleNmTasks] no relations found for task %d", task.ID)
	}

	return nil
}

func (core *Core) dequeueCommand(ctx context.Context, officeID pg_model.OfficeID, invoke pg_model.Call) error {
	err := core.commandService.ScheduleCommandFromQueue(ctx, officeID, invoke)
	if err != nil {
		return fmt.Errorf("[dequeueCommand] ScheduleCommandFromQueue failed: %w", err)
	}
	return nil
}

func (core *Core) dequeueNm(ctx context.Context, fn nomenclature.Call, officeID pg_model.OfficeID, taskType []string, ruinerInterval time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(ruinerInterval):
	}
	err := core.nomenclatureTopicService.ScheduleQueueElementWithBlock(ctx, fn, officeID, taskType)
	if err != nil {
		return fmt.Errorf("[dequeueNm] failed to schedule queue element with block: %w", err)
	}
	return nil
}

func (core *Core) computeQueueNumber(number, limit int64) int64 {
	n := number % limit
	return n
}
