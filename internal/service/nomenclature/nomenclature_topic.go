package nomenclature

import (
	"context"
	"errors"
	"fmt"

	"github.com/PavelAgarkov/template/internal/models"
	"github.com/PavelAgarkov/template/internal/models/pg_model"
	"github.com/PavelAgarkov/template/internal/repository/postgres"

	"github.com/jackc/pgx/v5"
)

type TopicService struct {
	nomenclatureTopicRepository postgres.NomenclatureTopicRepositoryInterface
	transactionManager          postgres.TransactionManagerInterface
}

func NewNomenclatureTopicService(
	transactionManager postgres.TransactionManagerInterface,
	nomenclatureTopicRepository postgres.NomenclatureTopicRepositoryInterface) *TopicService {
	return &TopicService{
		nomenclatureTopicRepository: nomenclatureTopicRepository,
		transactionManager:          transactionManager,
	}
}

func (t *TopicService) computeQueueNumber(officeID, officeLimit int64) int64 {
	return officeID % officeLimit
}

func (t *TopicService) PushNomenclatureGroupTask(ctx context.Context, tx pgx.Tx, bucket *pg_model.Bucket, push *pg_model.PushParams) error {
	if push.QueueNumber != 0 {
		return errors.New("QueueNumber must be 0 for group tasks")
	}

	queueNumber := t.computeQueueNumber(int64(bucket.OfficeID), models.NumberOfOffices)
	pushForOffice := &pg_model.PushParams{
		QueueNumber: queueNumber,
		TaskType:    push.TaskType,
		Priority:    push.Priority,
	}

	if pushForOffice.Priority == 0 {
		pushForOffice.Priority = pg_model.NomenclatureDefaultPushPriority
	}
	if pushForOffice.QueueNumber == 0 {
		pushForOffice.QueueNumber = pg_model.DefaultQueueNumber
	}
	if pushForOffice.TaskType == "" {
		pushForOffice.TaskType = pg_model.StreamTaskType
	}
	bucket.Push = pushForOffice

	err := t.nomenclatureTopicRepository.CreateNomenclatureGroupTask(ctx, tx, bucket)
	if err != nil {
		return fmt.Errorf("[PushNomenclatureGroupTask] CreateNomenclatureGroupTask failed: %w", err)
	}

	return nil
}

func (t *TopicService) GetRelationForStreamNomenclatureTask(ctx context.Context, tx pgx.Tx, task *pg_model.Task) ([]int64, error) {
	if task == nil {
		return nil, errors.New("task cannot be nil")
	}

	relations, err := t.nomenclatureTopicRepository.GetRelationForStreamNomenclatureTask(ctx, tx, task)
	if err != nil {
		return nil, fmt.Errorf("[GetRelationForStreamNomenclatureTask] GetRelationForStreamNomenclatureTask failed: %w", err)
	}
	if len(relations) == 0 {
		return nil, nil
	}
	return relations, nil
}

func (t *TopicService) ScheduleQueueElementWithBlock(
	ctx context.Context,
	fn Call,
	officeID pg_model.OfficeID,
	taskType []string,
) error {
	if officeID == 0 {
		return errors.New("officeID cannot be zero")
	}
	if len(taskType) == 0 {
		return errors.New("taskType cannot be empty")
	}

	err := t.transactionManager.CallWithTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		err := t.nomenclatureTopicRepository.ScheduleQueueElementWithBlock(ctx, tx, fn, officeID, taskType)
		if err != nil {
			return fmt.Errorf("[ScheduleQueueElementWithBlock] ScheduleQueueElementWithBlock failed: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("[ScheduleQueueElementWithBlock] CallWithTx failed: %w", err)
	}

	return nil
}
