package repository

import (
    "context"
    
    "github.com/go4s/util/convertor"
    "xorm.io/xorm"
)

type (
    Option interface {
        Decorate(*xorm.Session) (*xorm.Session, error)
    }
    OptionModifier[T Option] func(*T)
)

type (
    DeleteBehavior[T any, O Option, M OptionModifier[O]] interface {
        Delete(context.Context, ...M) (int64, error)
    }
)

type (
    UpdateBehavior[T any, O Option, M OptionModifier[O]] interface {
        Update(context.Context, convertor.Into[T], ...M) error
    }
    CreateBehavior[T any, O Option, M OptionModifier[O]] interface {
        Create(context.Context, convertor.Into[T], ...M) error
    }
    UpsertBehavior[T any, O Option, M OptionModifier[O]] interface {
        Upsert(context.Context, convertor.Into[T], ...M) error
    }
)

type (
    QueryBehavior[T any, O Option, M OptionModifier[O]] interface {
        Query(context.Context, convertor.From[T], ...M) error
    }
)

type (
    MigrationBehavior[T any, O Option, M OptionModifier[O]] interface {
        InjectEngine(context.Context, *xorm.Engine, ...M) error
    }
)
type (
    Repository[T any, O Option, M OptionModifier[O]] struct {
        engine *xorm.Engine
    }
)

func (r *Repository[T, O, M]) InjectEngine(ctx context.Context, engine *xorm.Engine, modifiers ...M) (err error) {
    var (
        schema = new(T)
        o      = new(O)
        sess   *xorm.Session
    )
    for _, modifier := range modifiers {
        modifier(o)
    }
    if r.engine = engine; o == nil {
        panic("option nil")
    }
    if sess, err = (*o).Decorate(r.engine.Context(ctx)); err != nil {
        return
    }
    if err = sess.Sync2(schema); err != nil {
        return
    }
    return sess.Close()
}
func (r *Repository[T, O, M]) setupSession(ctx context.Context, modifiers ...M) (sess *xorm.Session, err error) {
    var o O
    for _, modifier := range modifiers {
        modifier(&o)
    }
    return o.Decorate(r.engine.Context(ctx))
}
func (r *Repository[T, O, M]) Query(ctx context.Context, gather convertor.From[T], modifiers ...M) (err error) {
    var (
        sess *xorm.Session
    )
    if sess, err = r.setupSession(ctx, modifiers...); err != nil {
        return
    }
    var ts = []T{}
    if err = sess.Find(&ts); err != nil {
        return
    }
    for _, t := range ts {
        if err = gather.From(t); err != nil {
            return
        }
    }
    return
}
func (r *Repository[T, O, M]) Update(ctx context.Context, value convertor.Into[T], modifiers ...M) (err error) {
    var (
        val  T
        sess *xorm.Session
    )
    if sess, err = r.setupSession(ctx, modifiers...); err != nil {
        return
    }
    
    if val, err = value.Into(); err != nil {
        return
    }
    if _, err = sess.Update(val); err != nil {
        return
    }
    return
}
func (r *Repository[T, O, M]) Create(ctx context.Context, value convertor.Into[[]interface{}], modifiers ...M) (err error) {
    
    var (
        vals []interface{}
        sess *xorm.Session
    )
    if sess, err = r.setupSession(ctx, modifiers...); err != nil {
        return
    }
    
    if vals, err = value.Into(); err != nil || len(vals) == 0 {
        return
    }
    if _, err = sess.Insert(vals...); err != nil {
        return
    }
    return
}
func (r *Repository[T, O, M]) Upsert(ctx context.Context, value convertor.Into[T], modifiers ...M) (err error) {
	var (
		val T
		cnt int64
	)
	{
		var sess *xorm.Session
		if sess, err = r.setupSession(ctx, modifiers...); err != nil {
			return
		}
		if cnt, err = sess.Count(&val); err != nil {
			return
		}

	}
	if cnt == 0 {
		if _, err = r.engine.Context(ctx).Insert(value); err != nil {
			return
		}
		return
	}
	{
		var sess *xorm.Session
		if sess, err = r.setupSession(ctx, modifiers...); err != nil {
			return
		}
		if val, err = value.Into(); err != nil {
			return
		}
		if cnt, err = sess.Update(val); err != nil {
			return
		}
	}
	return
}
func (r *Repository[T, O, M]) Delete(ctx context.Context, modifiers ...M) (cnt int64, err error) {
    var (
        bean = new(T)
        sess *xorm.Session
    )
    if sess, err = r.setupSession(ctx, modifiers...); err != nil {
        return
    }
    cnt, err = sess.Delete(bean)
    return
}

func New[T any, O Option]() Repository[T, O, OptionModifier[O]] {
    return Repository[T, O, OptionModifier[O]]{}
}
