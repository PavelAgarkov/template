package command_bus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/PavelAgarkov/template/internal/config"
	"github.com/PavelAgarkov/template/internal/models"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	"github.com/PavelAgarkov/template/internal/repository/postgres"

	"github.com/jackc/pgx/v5"
)

type Command struct {
	commandTopicRepository postgres.CommandTopicRepositoryInterface
	config                 config.Config
	transactionManager     postgres.TransactionManagerInterface
}

func NewCommandService(
	config config.Config,
	commandTopicRepository postgres.CommandTopicRepositoryInterface,
	transactionManager postgres.TransactionManagerInterface,
) *Command {
	return &Command{
		config:                 config,
		commandTopicRepository: commandTopicRepository,
		transactionManager:     transactionManager,
	}
}

func (c *Command) ScheduleCommandFromQueue(ctx context.Context, officeID pg_model.OfficeID, invoke pg_model.Call) error {
	err := c.transactionManager.CallWithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		err := c.commandTopicRepository.Dequeue(ctx, tx, officeID, invoke)
		if err != nil {
			return fmt.Errorf("[ScheduleCommandFromQueue] failed to dequeue command topic for office %d: %w", officeID, err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("[ScheduleCommandFromQueue] CallWithTx failed: %w", err)
	}

	return nil
}

func (c *Command) Invoke(fnCtx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	invoke := c.InvokeRouter(cmd)
	if invoke != nil {
		err := invoke(fnCtx, tx, cmd)
		if err != nil {
			return fmt.Errorf("failed to call command %s for office %d: %w", cmd.Type, cmd.OfficeID, err)
		}
	}
	return nil
}

func (c *Command) InvokeRouter(cmd *pg_model.Command) pg_model.Call {
	switch cmd.Type {
	case pg_model.RecalculateCommandDyOfficeID, pg_model.RecalculateCommandByTtl:
		return c.recalculateCommandByOfficeID
	case pg_model.DeleteNmFromNomenclatureFilterScheduler:
		return c.scheduleDeleteNomenclatureFilter
	case pg_model.DeleteFromNomenclatureCacheTurnover:
		return c.deleteFromNomenclatureCacheTurnover
	}

	return nil
}

func (c *Command) deleteFromNomenclatureCacheTurnover(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	return nil
}

// Пример как можно использовать оффсет для длительного процесса обработки через команды.
func (c *Command) recalculateCommandByOfficeID(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	return nil
	//start := time.Now()
	//defer func() {
	//	logger_wrapper.WriteDebugLog(ctx, &logger_wrapper.LogEntry{
	//		Msg:       fmt.Sprintf("recalculateCommandByOfficeID() took time %s", time.Since(start)),
	//		Component: "command-service",
	//		Method:    "recalculateCommandByOfficeID",
	//		Start:     &start,
	//	})
	//}()
	//
	//offset := &models.GetTurnoverWithOffset{}
	//err := json.Unmarshal(cmd.Payload, offset)
	//if err != nil {
	//	return fmt.Errorf("[recalculateCommandByOfficeID] json.Unmarshal failed: %w", err)
	//}
	//
	//// один жирный бакет
	//takenBuckets, err := c.nomenclatureCache.GetNomenclatureIDsFromCacheWithOffset(ctx, tx, offset)
	//if err != nil {
	//	return fmt.Errorf("[recalculateCommandByOfficeID] GetAllNomenclatureIDs failed: %w", err)
	//}
	//if takenBuckets == nil || len(takenBuckets.Nms) == 0 {
	//	logger_wrapper.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
	//		Msg:       fmt.Sprintf("no NMs found for office %d", offset.OfficeID),
	//		Component: "command-service",
	//		Method:    "recalculateCommandByOfficeID",
	//		Args:      fmt.Sprintf("officeID: %d", offset.OfficeID),
	//	})
	//	// это естественное завершение обработки команды
	//	return nil
	//}
	//
	//var (
	//	taskTypeForNomenclature string
	//	priority                int64
	//)
	//if cmd.Type == pg_model.RecalculateCommandByTtl {
	//	taskTypeForNomenclature = pg_model.RecalculateByTtlTask
	//	priority = pg_model.NomenclaturePushPriorityHigh
	//}
	//
	//if cmd.Type == pg_model.RecalculateCommandDyOfficeID {
	//	taskTypeForNomenclature = pg_model.RecalculateByOfficeIDTask
	//	priority = pg_model.NomenclaturePushPriorityImmediate
	//}
	//
	//// тут его распиливаем на takenBuckets.Nms / pg_model.TaskNMButch
	//groupedBuckets := c.nmSeparator.GroupNmsToBuckets(takenBuckets)
	//push := &pg_model.PushParams{
	//	Priority: priority,
	//	TaskType: taskTypeForNomenclature,
	//}
	////тут некуда спешить, пусть запускаюстя по очереди иначе тут будет сотня горутин
	//for _, buckets := range groupedBuckets {
	//	for _, bucket := range buckets {
	//		err := c.nomenclatureCache.ProcessNMSForOfficeIdWithoutDifference(ctx, tx, bucket, push)
	//		if err != nil {
	//			logger_wrapper.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
	//				Msg:       "failed to process new NMS",
	//				Component: "command-service",
	//				Method:    "recalculateCommandByOfficeID",
	//				Args:      fmt.Sprintf("bucket: %v, officeID: %d", bucket, cmd.OfficeID),
	//				Error:     err,
	//			})
	//			return fmt.Errorf("[recalculateCommandByOfficeID] ProcessNMSForOfficeIdWithoutDifference: %w", err)
	//		}
	//	}
	//}
	//
	//offset.LastNmID = takenBuckets.Nms[len(takenBuckets.Nms)-1]
	//cmd.Payload, err = json.Marshal(offset)
	//err = c.CreateCommandWithTx(ctx, tx, cmd)
	//if err != nil {
	//	logger_wrapper.WriteErrorLog(ctx, &logger_wrapper.LogEntry{
	//		Msg:       "failed to create command for next iteration",
	//		Component: "command-service",
	//		Method:    "recalculateCommandByOfficeID",
	//		Args:      fmt.Sprintf("officeID: %d", cmd.OfficeID),
	//		Error:     err,
	//	})
	//	return fmt.Errorf("[recalculateCommandByOfficeID] CreateCommand failed: %w", err)
	//}
	//
	//logger_wrapper.WriteInfoLog(ctx, &logger_wrapper.LogEntry{
	//	Msg:       fmt.Sprintf("we have finished processing all NMS for office %v", offset.OfficeID),
	//	Component: "command-service",
	//	Method:    "recalculateCommandByOfficeID",
	//})
	//
	//return nil
}

func (c *Command) scheduleDeleteNomenclatureFilter(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	return nil
}

func (c *Command) CreateCommand(ctx context.Context, cmd *pg_model.Command) error {
	if cmd == nil {
		return fmt.Errorf("[CreateCommand] command is nil")
	}

	err := c.transactionManager.CallWithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := c.commandTopicRepository.Enqueue(ctx, tx, cmd); err != nil {
			return fmt.Errorf("[CreateCommand] failed to enqueue command: %w", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("[CreateCommand] CallWithTx failed: %w", err)
	}

	return nil
}

func (c *Command) CreateCommandWithTx(ctx context.Context, tx pgx.Tx, cmd *pg_model.Command) error {
	if cmd == nil {
		return fmt.Errorf("[CreateCommand] command is nil")
	}

	if err := c.commandTopicRepository.Enqueue(ctx, tx, cmd); err != nil {
		return fmt.Errorf("[CreateCommand] failed to enqueue command: %w", err)
	}

	return nil
}

func (c *Command) GetCommand(ctx context.Context, queueNumber int64, cmdType string) (*pg_model.Command, error) {
	cmd, err := c.commandTopicRepository.GetCommand(ctx, queueNumber, cmdType)
	if err != nil {
		return nil, fmt.Errorf("[GetCommand] failed to get command for office %d: %w", queueNumber, err)
	}
	if cmd == nil {
		return nil, nil
	}

	return cmd, nil
}

func CreateStartOffset(officeID pg_model.OfficeID) ([]byte, error) {
	offset := &models.GetTurnoverWithOffset{
		OfficeID: officeID,
		Offset:   pg_model.TaskNMButch,
		LastNmID: 1,
	}
	jsonOffset, err := json.Marshal(offset)
	if err != nil {
		return nil, fmt.Errorf("[RecalculateTurnoverForOffice] json.Marshal failed: %w", err)
	}
	return jsonOffset, nil
}
