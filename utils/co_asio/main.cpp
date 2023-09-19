#include <concepts>
#include <coroutine>
#include <exception>
#include <iostream>
#include <__coroutine/coroutine_handle.h>
#include <__coroutine/trivial_awaitables.h>

struct ReturnObject
{
    struct promise_type
    {
        ReturnObject get_return_object() { return {}; }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
    };
};

struct Awaiter
{
    std::coroutine_handle<> *hp;
    constexpr bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) { *hp = h; }
    constexpr void await_resume() const noexcept {}
};

ReturnObject counter(std::coroutine_handle<> * continuation_out)
{
    Awaiter a{continuation_out};

    for (unsigned i = 0;; ++i)
    {
        co_await a;
        std::cout << "counter: " << i << std::endl;
    }
}

struct ReturnObject2
{
    struct promise_type
    {
        ReturnObject2 get_return_object()
        {
            return
            {
                .h_ = std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
    };

    std::coroutine_handle<promise_type> h_;
    operator std::coroutine_handle<promise_type>() const { return h_; }
    operator std::coroutine_handle<>() const { return h_; }
};

ReturnObject2 counter2()
{
    for (unsigned i = 0;; ++i)
    {
        co_await std::suspend_always{};
        std::cout << "counter2: " << i << std::endl;
    }
}

template <typename PromiseType>
struct GetPromise
{
    PromiseType * p_;
    bool await_ready() { return false; }
    bool await_suspend(std::coroutine_handle<PromiseType> h)
    {
        p_ = &h.promise();
        return false;
    }

    PromiseType * await_resume() { return p_; }
};

struct ReturnObject3
{
    struct promise_type
    {
        unsigned value_;

        ReturnObject3 get_return_object()
        {

            return
            {
                .h_ = std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
    };

    std::coroutine_handle<promise_type> h_;
    operator std::coroutine_handle<promise_type>() const { return h_; }
};

ReturnObject3 counter3()
{
    auto * pp = co_await GetPromise<ReturnObject3::promise_type>{};
    for (unsigned i = 0;; ++i)
    {
        pp->value_ = i;
        co_await std::suspend_always{};
    }
}

struct ReturnObject4
{
    struct promise_type
    {
        unsigned value_;

        ReturnObject4 get_return_object()
        {

            return
            {
                .h_ = std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
        std::suspend_always yield_value(unsigned value)
        {
            value_ = value;
            return {};
        }
    };

    std::coroutine_handle<promise_type> h_;
};

ReturnObject4 counter4()
{
    for (unsigned i = 0;; ++i)
        co_yield i;
}

struct ReturnObject5
{
    struct promise_type
    {
        unsigned value_;

        ~promise_type()
        {
            std::cout << "promise_type destroyed" << std::endl;
        }

        ReturnObject5 get_return_object()
        {

            return
            {
                .h_ = std::coroutine_handle<promise_type>::from_promise(*this)
            };
        }
        std::suspend_never initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
        std::suspend_always yield_value(unsigned value)
        {
            value_ = value;
            return {};
        }

        void return_void() {}
    };

    std::coroutine_handle<promise_type> h_;
};

ReturnObject5 counter5()
{
    for (unsigned i = 0; i < 3; ++i)
        co_yield i;
}

template <typename T>
struct Generator
{
    struct promise_type;
    using handle_type = std::coroutine_handle<promise_type>;

    struct promise_type
    {
        T value_;
        std::exception_ptr exception_;

        Generator get_return_object()
        {
            return Generator(handle_type::from_promise(*this));
        }

        std::suspend_always initial_suspend() { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() { exception_ = std::current_exception(); }
        
        template <std::convertible_to<T> From>
        std::suspend_always yield_value(From && from)
        {
            value_ = std::forward<From>(from);
            return {};
        }

        void return_void() {}
    };

    handle_type h_;

    Generator(handle_type h) : h_(h) {}
    Generator(const Generator &) = delete;
    ~Generator() { h_.destroy(); }
    explicit operator bool()
    {
        fill();
        return !h_.done();
    }

    T operator()()
    {
        fill();
        full_ = false;
        return std::move(h_.promise().value_);
    }

private:
    bool full_ = false;

    void fill()
    {
        if (!full_)
        {
            h_();
            if (h_.promise().exception_)
                std::rethrow_exception(h_.promise().exception_);

            full_ = true;
        }
    }
};

Generator<unsigned> counter6()
{
    for (unsigned i = 0; i < 3; ++i)
        co_yield i;
}

int main()
{
    //std::coroutine_handle<> h;
    //counter(&h);
    //for (int i = 0; i < 3; ++i)
    //{
    //    std::cout << "In main1 function\n";
    //    h();
    //}
    //h.destroy();

    //std::coroutine_handle<> h  = counter2();
    //for (int i = 0; i < 3; ++i)
    //{
    //    std::cout << "In main2 function\n";
    //    h();
    //}
    //h.destroy();

    //std::coroutine_handle<ReturnObject3::promise_type> h = counter3();
    //ReturnObject3::promise_type & promise = h.promise();
    //for (int i = 0; i < 3; ++i)
    //{
    //    std::cout << "counter3: " << promise.value_ << std::endl;
    //    h();
    //}
    //h.destroy();

    //auto h = counter4().h_;
    //auto & promise = h.promise();
    //for (int i = 0; i < 3; ++i)
    //{
    //    std::cout << "counter4: " << promise.value_ << std::endl;
    //    h();
    //}
    //h.destroy();

    //auto h = counter5().h_;
    //auto & promise = h.promise();
    //while (!h.done())
    //{
    //    std::cout << "counter5: " << promise.value_ << std::endl;
    //    h();
    //}
    //h.destroy();

    auto gen = counter6();
    while (gen)
        std::cout << "counter6: " << gen() << std::endl;
}
