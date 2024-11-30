package edge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/models"
)

// Message represents data to be passed along an edge.
// Messages can be shared across many contexts.
//
// All messages implement their own ShallowCopy method.
// All ShallowCopy methods create a copy of the message but does not
// deeply copy any reference types.
//
// Never mutate a reference type returned from a message without first directly copying
// the reference type.
type Message interface {
	// Type returns the type of the message.
	Type() MessageType
	//TODO(nathanielc): Explore adding a shared flag to Messages to check when they have been shared.
	// Then we can enforce shared messages cannot be mutated.
	//markShared()
}

type MessageType int

const (
	BeginBatch MessageType = iota
	BatchPoint
	EndBatch
	BufferedBatch
	Point
	Barrier
	DeleteGroup
)

type GroupIDGetter interface {
	GroupID() models.GroupID
}

type GroupInfoer interface {
	GroupIDGetter
	GroupInfo() GroupInfo
}

type NameGetter interface {
	Name() string
}
type NameSetter interface {
	NameGetter
	SetName(string)
}

type DimensionGetter interface {
	Dimensions() models.Dimensions
}
type DimensionSetter interface {
	DimensionGetter
	SetDimensions(models.Dimensions)
}

type TimeGetter interface {
	Time() time.Time
}
type TimeSetter interface {
	TimeGetter
	SetTime(time.Time)
}

type FieldGetter interface {
	Fields() models.Fields
}
type FieldSetter interface {
	FieldGetter
	SetFields(models.Fields)
}

type TagGetter interface {
	Tags() models.Tags
}
type TagSetter interface {
	TagGetter
	SetTags(models.Tags)
}

type FieldsTagsTimeSetter interface {
	FieldSetter
	TagSetter
	TimeSetter
}

type FieldsTagsTimeGetter interface {
	FieldGetter
	TagGetter
	TimeGetter
}

type FieldsTagsTimeGetterMessage interface {
	Message
	FieldsTagsTimeGetter
}

// PointMeta is the common read interfaces of point and batch messages.
type PointMeta interface {
	NameGetter
	GroupInfoer
	DimensionGetter
	TagGetter
	TimeGetter
}

func (m MessageType) String() string {
	switch m {
	case BeginBatch:
		return "begin_batch"
	case BatchPoint:
		return "batch_point"
	case EndBatch:
		return "end_batch"
	case BufferedBatch:
		return "buffered_batch"
	case Point:
		return "point"
	case Barrier:
		return "barrier"
	default:
		return fmt.Sprintf("unknown message type %d", int(m))
	}
}

// PointMessage is a single point.
type PointMessage interface {
	Message

	ShallowCopy() PointMessage

	NameSetter

	Database() string
	SetDatabase(string)
	RetentionPolicy() string
	SetRetentionPolicy(string)

	GroupInfoer

	DimensionSetter
	SetTagsAndDimensions(models.Tags, models.Dimensions)

	FieldsTagsTimeSetter

	Bytes(precision string) []byte

	ToResult() models.Result
	ToRow() *models.Row
}

type pointMessage struct {
	name            string
	database        string
	retentionPolicy string

	groupID    models.GroupID
	dimensions models.Dimensions

	tags models.Tags

	fields models.Fields

	time time.Time
}

func NewPointMessage(
	name,
	database,
	retentionPolicy string,
	dimensions models.Dimensions,
	fields models.Fields,
	tags models.Tags,
	time time.Time) PointMessage {
	pm := &pointMessage{
		name:            name,
		database:        database,
		retentionPolicy: retentionPolicy,
		dimensions:      dimensions,
		groupID:         models.ToGroupID(name, tags, dimensions),
		tags:            tags,
		fields:          fields,
		time:            time,
	}
	return pm
}

func (pm *pointMessage) ShallowCopy() PointMessage {
	c := new(pointMessage)
	*c = *pm
	return c
}

func (*pointMessage) Type() MessageType {
	return Point
}

func (pm *pointMessage) Name() string {
	return pm.name
}
func (pm *pointMessage) SetName(name string) {
	pm.name = name
	pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
}
func (pm *pointMessage) Database() string {
	return pm.database
}
func (pm *pointMessage) SetDatabase(database string) {
	pm.database = database
}
func (pm *pointMessage) RetentionPolicy() string {
	return pm.retentionPolicy
}
func (pm *pointMessage) SetRetentionPolicy(retentionPolicy string) {
	pm.retentionPolicy = retentionPolicy
}
func (pm *pointMessage) GroupID() models.GroupID {
	return pm.groupID
}
func (pm *pointMessage) GroupInfo() GroupInfo {
	tags := make(models.Tags, len(pm.dimensions.TagNames))
	for _, t := range pm.dimensions.TagNames {
		tags[t] = pm.tags[t]
	}
	return GroupInfo{
		ID:         pm.groupID,
		Tags:       tags,
		Dimensions: pm.dimensions,
	}
}

func (pm *pointMessage) Dimensions() models.Dimensions {
	return pm.dimensions
}
func (pm *pointMessage) SetDimensions(dimensions models.Dimensions) {
	if !pm.dimensions.Equal(dimensions) {
		pm.dimensions = dimensions
		pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
	}
}
func (pm *pointMessage) Tags() models.Tags {
	return pm.tags
}
func (pm *pointMessage) SetTags(tags models.Tags) {
	pm.tags = tags
	pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
}

func (pm *pointMessage) SetTagsAndDimensions(tags models.Tags, dimensions models.Dimensions) {
	pm.dimensions = dimensions
	pm.tags = tags
	pm.groupID = models.ToGroupID(pm.name, pm.tags, pm.dimensions)
}
func (pm *pointMessage) Fields() models.Fields {
	return pm.fields
}
func (pm *pointMessage) SetFields(fields models.Fields) {
	pm.fields = fields
}
func (pm *pointMessage) Time() time.Time {
	return pm.time
}
func (pm *pointMessage) SetTime(time time.Time) {
	pm.time = time
}

// Returns byte array of a line protocol representation of the point
func (pm *pointMessage) Bytes(precision string) []byte {
	key := imodels.MakeKey([]byte(pm.name), imodels.NewTags(pm.tags))
	fields := imodels.Fields(pm.fields).MarshalBinary()
	kl := len(key)
	fl := len(fields)
	var bytes []byte

	if pm.time.IsZero() {
		bytes = make([]byte, fl+kl+1)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
	} else {
		timeStr := strconv.FormatInt(pm.time.UnixNano()/imodels.GetPrecisionMultiplier(precision), 10)
		tl := len(timeStr)
		bytes = make([]byte, fl+kl+tl+2)
		copy(bytes, key)
		bytes[kl] = ' '
		copy(bytes[kl+1:], fields)
		bytes[kl+fl+1] = ' '
		copy(bytes[kl+fl+2:], []byte(timeStr))
	}

	return bytes
}

func (pm *pointMessage) ToResult() models.Result {
	return models.Result{
		Series: models.Rows{pm.ToRow()},
	}
}
func (pm *pointMessage) ToRow() *models.Row {
	row := &models.Row{
		Name: pm.name,
		Tags: pm.tags,
	}
	row.Columns = make([]string, 1, len(pm.fields)+1)
	row.Columns[0] = "time"
	for f := range pm.fields {
		row.Columns = append(row.Columns, f)
	}
	// Sort all columns but leave time as first
	sort.Strings(row.Columns[1:])

	row.Values = make([][]interface{}, 1)
	row.Values[0] = make([]interface{}, len(row.Columns))
	row.Values[0][0] = pm.time
	for i, c := range row.Columns[1:] {
		if v, ok := pm.fields[c]; ok {
			row.Values[0][i+1] = v
		}
	}
	return row
}

type pointMessageJSON struct {
	Name            string            `json:"name,omitempty"`
	Database        string            `json:"database,omitempty"`
	RetentionPolicy string            `json:"retentionPolicy,omitempty"`
	Group           models.GroupID    `json:"group,omitempty"`
	Dimensions      models.Dimensions `json:"dimensions,omitempty"`
	Fields          models.Fields     `json:"fields,omitempty"`
	Tags            models.Tags       `json:"tags,omitempty"`
	Time            time.Time         `json:"time,omitempty"`
}

func (pm *pointMessage) MarshalJSON() ([]byte, error) {
	p := pointMessageJSON{
		Name:            pm.name,
		Database:        pm.database,
		RetentionPolicy: pm.retentionPolicy,
		Group:           pm.groupID,
		Dimensions:      pm.dimensions,
		Fields:          pm.fields,
		Tags:            pm.tags,
		Time:            pm.time,
	}
	return json.Marshal(p)
}

// BeginBatchMessage marks the beginning of a batch of points.
// Once a BeginBatchMessage is received all subsequent message will be BatchPointMessages until an EndBatchMessage is received.
type BeginBatchMessage interface {
	Message

	ShallowCopy() BeginBatchMessage

	NameSetter

	GroupInfoer
	TagSetter
	DimensionSetter
	SetTagsAndDimensions(models.Tags, models.Dimensions)

	// Time is the maximum time of any point in the batch
	TimeSetter

	// SizeHint provides a hint about the size of the batch to come.
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	SizeHint() int
	SetSizeHint(int)
}

type beginBatchMessage struct {
	name       string
	groupID    models.GroupID
	tags       models.Tags
	dimensions models.Dimensions
	tmax       time.Time
	// If non-zero expect a batch with SizeHint points,
	// otherwise an unknown number of points are coming.
	sizeHint int
}

func NewBeginBatchMessage(
	name string,
	tags models.Tags,
	byName bool,
	tmax time.Time,
	sizeHint int,
) BeginBatchMessage {
	dimensions := models.Dimensions{
		TagNames: models.SortedKeys(tags),
		ByName:   byName,
	}
	groupID := models.ToGroupID(name, tags, dimensions)
	bb := &beginBatchMessage{
		name:       name,
		tags:       tags,
		dimensions: dimensions,
		groupID:    groupID,
		tmax:       tmax,
		sizeHint:   sizeHint,
	}
	return bb
}

func (beginBatchMessage) Type() MessageType {
	return BeginBatch
}

func (bb *beginBatchMessage) ShallowCopy() BeginBatchMessage {
	c := new(beginBatchMessage)
	*c = *bb
	return c
}

func (bb *beginBatchMessage) Name() string {
	return bb.name
}
func (bb *beginBatchMessage) SetName(name string) {
	bb.name = name
	bb.groupID = models.ToGroupID(bb.name, bb.tags, bb.dimensions)
}
func (bb *beginBatchMessage) GroupID() models.GroupID {
	return bb.groupID
}
func (bb *beginBatchMessage) GroupInfo() GroupInfo {
	return GroupInfo{
		ID:         bb.groupID,
		Tags:       bb.tags,
		Dimensions: bb.dimensions,
	}
}
func (bb *beginBatchMessage) Tags() models.Tags {
	return bb.tags
}

// SetTags updates the tags on the message.
// The dimensions are also updated to reflect the new tags.
func (bb *beginBatchMessage) SetTags(tags models.Tags) {
	bb.tags = tags
	bb.dimensions.TagNames = models.SortedKeys(tags)
	bb.groupID = models.ToGroupID(bb.name, bb.tags, bb.dimensions)
}

func (bb *beginBatchMessage) Dimensions() models.Dimensions {
	return bb.dimensions
}

// SetDimensions updates the dimensions on the message.
// The tags are updated to reflect the new dimensions.
// If new dimensions are being added use SetTags instead as the dimensions will be automatically updated.
func (bb *beginBatchMessage) SetDimensions(dimensions models.Dimensions) {
	if !bb.dimensions.Equal(dimensions) {
		bb.SetTagsAndDimensions(bb.tags, dimensions)
	}
}

// SetTagsAndDimensions updates both tags and dimensions at the same time.
// The tags will be updated to make sure they match the new dimensions.
func (bb *beginBatchMessage) SetTagsAndDimensions(tags models.Tags, dimensions models.Dimensions) {
	newTags := make(models.Tags, len(tags))
	for _, dim := range dimensions.TagNames {
		newTags[dim] = tags[dim]
	}
	bb.tags = newTags
	bb.dimensions = dimensions
	bb.groupID = models.ToGroupID(bb.name, bb.tags, bb.dimensions)
}
func (bb *beginBatchMessage) Time() time.Time {
	return bb.tmax
}
func (bb *beginBatchMessage) SetTime(tmax time.Time) {
	bb.tmax = tmax
}

func (bb *beginBatchMessage) SizeHint() int {
	return bb.sizeHint
}
func (bb *beginBatchMessage) SetSizeHint(sizeHint int) {
	bb.sizeHint = sizeHint
}

// BatchPointMessage is a single point in a batch of data.
type BatchPointMessage interface {
	Message

	ShallowCopy() BatchPointMessage

	FieldsTagsTimeSetter
}

type batchPointMessage struct {
	fields models.Fields
	tags   models.Tags
	time   time.Time
}

func NewBatchPointMessage(
	fields models.Fields,
	tags models.Tags,
	time time.Time,
) BatchPointMessage {
	return &batchPointMessage{
		fields: fields,
		tags:   tags,
		time:   time,
	}
}

func (*batchPointMessage) Type() MessageType {
	return BatchPoint
}
func (bp *batchPointMessage) ShallowCopy() BatchPointMessage {
	c := new(batchPointMessage)
	*c = *bp
	return c
}

func (bp *batchPointMessage) Fields() models.Fields {
	return bp.fields
}
func (bp *batchPointMessage) SetFields(fields models.Fields) {
	bp.fields = fields
}
func (bp *batchPointMessage) Tags() models.Tags {
	return bp.tags
}
func (bp *batchPointMessage) SetTags(tags models.Tags) {
	bp.tags = tags
}
func (bp *batchPointMessage) Time() time.Time {
	return bp.time
}
func (bp *batchPointMessage) SetTime(time time.Time) {
	bp.time = time
}

func BatchPointFromPoint(p PointMessage) BatchPointMessage {
	return NewBatchPointMessage(
		p.Fields(),
		p.Tags(),
		p.Time(),
	)
}

// EndBatchMessage indicates that all points for a batch have arrived.
type EndBatchMessage interface {
	Message

	ShallowCopy() EndBatchMessage
}

type endBatchMessage struct {
}

func NewEndBatchMessage() EndBatchMessage {
	return &endBatchMessage{}
}

func (*endBatchMessage) Type() MessageType {
	return EndBatch
}
func (eb *endBatchMessage) ShallowCopy() EndBatchMessage {
	c := new(endBatchMessage)
	*c = *eb
	return c
}

// BufferedBatchMessage is a message containing all data for a single batch.
type BufferedBatchMessage interface {
	Message

	ShallowCopy() BufferedBatchMessage

	Begin() BeginBatchMessage
	SetBegin(BeginBatchMessage)

	// Expose common read interfaces of begin and point messages.
	PointMeta

	Points() []BatchPointMessage
	SetPoints([]BatchPointMessage)

	End() EndBatchMessage
	SetEnd(EndBatchMessage)

	ToResult() models.Result
	ToRow() *models.Row
}

type bufferedBatchMessage struct {
	begin  BeginBatchMessage
	points []BatchPointMessage
	end    EndBatchMessage
}

func NewBufferedBatchMessage(
	begin BeginBatchMessage,
	points []BatchPointMessage,
	end EndBatchMessage,
) BufferedBatchMessage {
	return &bufferedBatchMessage{
		begin:  begin,
		points: points,
		end:    end,
	}
}

func (*bufferedBatchMessage) Type() MessageType {
	return BufferedBatch
}
func (bb *bufferedBatchMessage) ShallowCopy() BufferedBatchMessage {
	c := new(bufferedBatchMessage)
	*c = *bb
	return c
}
func (bb *bufferedBatchMessage) Begin() BeginBatchMessage {
	return bb.begin
}
func (bb *bufferedBatchMessage) SetBegin(begin BeginBatchMessage) {
	bb.begin = begin
}

func (bb *bufferedBatchMessage) Name() string {
	return bb.begin.Name()
}
func (bb *bufferedBatchMessage) GroupID() models.GroupID {
	return bb.begin.GroupID()
}
func (bb *bufferedBatchMessage) GroupInfo() GroupInfo {
	return bb.begin.GroupInfo()
}
func (bb *bufferedBatchMessage) Dimensions() models.Dimensions {
	return bb.begin.Dimensions()
}
func (bb *bufferedBatchMessage) Tags() models.Tags {
	return bb.begin.Tags()
}
func (bb *bufferedBatchMessage) Time() time.Time {
	return bb.begin.Time()
}

func (bb *bufferedBatchMessage) Points() []BatchPointMessage {
	return bb.points
}
func (bb *bufferedBatchMessage) SetPoints(points []BatchPointMessage) {
	bb.points = points
}
func (bb *bufferedBatchMessage) End() EndBatchMessage {
	return bb.end
}
func (bb *bufferedBatchMessage) SetEnd(end EndBatchMessage) {
	bb.end = end
}

func (bb *bufferedBatchMessage) ToResult() models.Result {
	return models.Result{
		Series: models.Rows{bb.ToRow()},
	}
}
func (bb *bufferedBatchMessage) ToRow() (row *models.Row) {
	row = &models.Row{
		Name: bb.begin.Name(),
		Tags: bb.begin.Tags(),
	}
	if len(bb.points) == 0 {
		return
	}
	row.Columns = []string{"time"}

	tagNames := make(map[string]struct{})
	fieldNames := make(map[string]struct{})
	// we have to walk all the points to get the columns.
	for _, p := range bb.points {
		for f := range p.Fields() {
			fieldNames[f] = struct{}{}
		}

		// Append tags that are not on the batch
		for t := range p.Tags() {
			if _, ok := bb.begin.Tags()[t]; !ok {
				tagNames[t] = struct{}{}
			}
		}
	}
	for f := range fieldNames {
		row.Columns = append(row.Columns, f)
	}

	// Append tags that are not on the batch
	for t := range tagNames {
		if _, ok := bb.begin.Tags()[t]; !ok {
			row.Columns = append(row.Columns, t)
		}
	}

	// Sort all columns but leave time as first
	sort.Strings(row.Columns[1:])
	row.Values = make([][]interface{}, len(bb.points))
	for i, p := range bb.points {
		row.Values[i] = make([]interface{}, len(row.Columns))
		row.Values[i][0] = p.Time()
		for j, c := range row.Columns[1:] {
			if v, ok := p.Fields()[c]; ok {
				row.Values[i][j+1] = v
			} else if v, ok := p.Tags()[c]; ok {
				row.Values[i][j+1] = v
			}
		}
	}
	return
}

type bufferedBatchMessageJSON struct {
	Name   string                  `json:"name,omitempty"`
	TMax   time.Time               `json:"tmax,omitempty"`
	Group  models.GroupID          `json:"group,omitempty"`
	ByName bool                    `json:"byname,omitempty"`
	Tags   models.Tags             `json:"tags,omitempty"`
	Points []batchPointMessageJSON `json:"points,omitempty"`
}

type batchPointMessageJSON struct {
	Fields models.Fields `json:"fields"`
	Tags   models.Tags   `json:"tags"`
	Time   time.Time     `json:"time"`
}

type BufferedBatchMessageDecoder interface {
	Decode() (BufferedBatchMessage, error)
	More() bool
}

type bufferedBatchMessageDecoder struct {
	dec *json.Decoder
}

func (d *bufferedBatchMessageDecoder) More() bool {
	return d.dec.More()
}

func (d *bufferedBatchMessageDecoder) Decode() (BufferedBatchMessage, error) {
	bb := &bufferedBatchMessage{
		begin: new(beginBatchMessage),
		end:   new(endBatchMessage),
	}
	err := d.dec.Decode(bb)
	return bb, err
}

func NewBufferedBatchMessageDecoder(r io.Reader) BufferedBatchMessageDecoder {
	return &bufferedBatchMessageDecoder{
		dec: json.NewDecoder(r),
	}
}

func (bb *bufferedBatchMessage) MarshalJSON() ([]byte, error) {
	b := &bufferedBatchMessageJSON{
		Name:   bb.begin.Name(),
		TMax:   bb.begin.Time(),
		Group:  bb.begin.GroupID(),
		ByName: bb.begin.Dimensions().ByName,
		Tags:   bb.begin.Tags(),
		Points: make([]batchPointMessageJSON, len(bb.points)),
	}
	for i := range b.Points {
		b.Points[i] = batchPointMessageJSON{
			Fields: bb.points[i].Fields(),
			Tags:   bb.points[i].Tags(),
			Time:   bb.points[i].Time(),
		}
	}
	return json.Marshal(b)
}

func (bb *bufferedBatchMessage) UnmarshalJSON(data []byte) error {
	b := new(bufferedBatchMessageJSON)
	json.Unmarshal(data, &b)
	bb.begin.SetName(b.Name)
	bb.begin.SetTags(b.Tags)
	dims := bb.begin.Dimensions()
	dims.ByName = b.ByName
	bb.begin.SetDimensions(dims)
	bb.begin.SetTime(b.TMax.UTC())
	bb.begin.SetSizeHint(len(b.Points))
	bb.points = make([]BatchPointMessage, len(b.Points))
	for i := range bb.points {
		tags := b.Points[i].Tags
		if len(tags) == 0 {
			tags = b.Tags
		}
		bb.points[i] = NewBatchPointMessage(
			b.Points[i].Fields,
			tags,
			b.Points[i].Time.UTC(),
		)
	}
	return nil
}

func ResultToBufferedBatches(res influxdb.Result, groupByName bool) ([]BufferedBatchMessage, error) {
	if res.Err != "" {
		return nil, errors.New(res.Err)
	}
	batches := make([]BufferedBatchMessage, 0, len(res.Series))
	for _, series := range res.Series {
		b := NewBufferedBatchMessage(
			NewBeginBatchMessage(
				series.Name,
				series.Tags,
				groupByName,
				time.Time{},
				len(series.Values),
			),
			make([]BatchPointMessage, 0, len(series.Values)),
			NewEndBatchMessage(),
		)
		points := b.Points()

		for _, v := range series.Values {
			fields := make(models.Fields)
			var t time.Time
			for i, c := range series.Columns {
				if c == "time" {
					//tStr, ok := v[i].(string)
					switch ts := v[i].(type) {
					case string:
						var err error
						t, err = time.Parse(time.RFC3339Nano, ts)
						if err != nil {
							t, err = time.Parse(time.RFC3339, ts)
							if err != nil {
								return nil, fmt.Errorf("unexpected time format: %v", err)
							}
						}
					case time.Time:
						t = ts
					default:
						return nil, fmt.Errorf("unexpected time value: %v", v[i])
					}
				} else {
					value := v[i]
					if n, ok := value.(json.Number); ok {
						f, err := n.Float64()
						if err == nil {
							value = f
						}
					}
					if value == nil {
						continue
					}
					fields[c] = value
				}
			}
			if len(fields) > 0 {
				if t.After(b.Begin().Time()) {
					b.Begin().SetTime(t.UTC())
				}
				points = append(
					points,
					NewBatchPointMessage(
						fields,
						series.Tags,
						t.UTC(),
					),
				)
			}
		}
		b.Begin().SetSizeHint(len(points))
		b.SetPoints(points)
		batches = append(batches, b)
	}
	return batches, nil
}

type BatchPointMessages []BatchPointMessage

func (l BatchPointMessages) Len() int               { return len(l) }
func (l BatchPointMessages) Less(i int, j int) bool { return l[i].Time().Before(l[j].Time()) }
func (l BatchPointMessages) Swap(i int, j int)      { l[i], l[j] = l[j], l[i] }

// BarrierMessage indicates that no data older than the barrier time will arrive.
type BarrierMessage interface {
	Message
	ShallowCopy() BarrierMessage
	GroupInfoer
	NameGetter
	DimensionGetter
	TagGetter
	TimeGetter
}
type barrierMessage struct {
	group GroupInfo
	time  time.Time
}

func NewBarrierMessage(group GroupInfo, time time.Time) BarrierMessage {
	return &barrierMessage{
		group: group,
		time:  time,
	}
}

func (b *barrierMessage) ShallowCopy() BarrierMessage {
	c := new(barrierMessage)
	*c = *b
	return c
}

func (*barrierMessage) Name() string {
	return "barrier"
}

func (*barrierMessage) Dimensions() models.Dimensions {
	return models.Dimensions{}
}

func (*barrierMessage) Tags() models.Tags {
	return models.Tags{}
}

func (*barrierMessage) Type() MessageType {
	return Barrier
}
func (b *barrierMessage) GroupID() models.GroupID {
	return b.group.ID
}
func (b *barrierMessage) GroupInfo() GroupInfo {
	return b.group
}
func (b *barrierMessage) Time() time.Time {
	return b.time
}

type DeleteGroupMessage interface {
	Message
	GroupInfoer
}

type deleteGroupMessage struct {
	info GroupInfo
}

func NewDeleteGroupMessage(info GroupInfo) *deleteGroupMessage {
	return &deleteGroupMessage{
		info: info,
	}
}

func (d *deleteGroupMessage) Type() MessageType {
	return DeleteGroup
}

func (d *deleteGroupMessage) GroupID() models.GroupID {
	return d.info.ID
}

func (d *deleteGroupMessage) GroupInfo() GroupInfo {
	return d.info
}
