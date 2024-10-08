#include "context.hpp"
#include "coroutine.hpp"

#include <memory>
#include <iostream>

void sample_contexts() {
    std::cout << "-----------Contexts-----------" << std::endl;

    static AIO::aio_context context { };
    constexpr static std::size_t STACK_SIZE_BYTES = 16 * 1024; // 16 KiB

    const auto stack = std::make_unique<char[]>(STACK_SIZE_BYTES);

    auto subcontext_entrypoint = []() -> void {
        std::cout << "Hello from subcontext" << std::endl;
        aio_context_switch(&context);
        std::cout << "Subcontext will exit now" << std::endl;
    };

    aio_context_create(&context, stack.get(), STACK_SIZE_BYTES, subcontext_entrypoint);

    std::cout << "Hello from main" << std::endl;
    aio_context_switch(&context);
    std::cout << "Finishing the subcontext" << std::endl;
    aio_context_switch(&context);
    std::cout << "Done" << std::endl;
}

void sample_coroutines() {
    std::cout << "----------Coroutines----------" << std::endl;

    constexpr int MAX = 100000;
    AIO::Coroutine<int()> fib = [&fib] [[noreturn]] () -> int {
        int prev = 0, cur = 1;
        while (true) {
            if (cur > MAX)
                throw AIO::EndGeneration();

            fib.yield(cur);
            const int next = prev + cur;
            prev = cur;
            cur = next;
        }
    };

    constexpr size_t N = 10;
    for (size_t i = 1; i <= N; i++) {
        std::cout << "fib[" << i << "] = " << fib.resume() << std::endl;
    }

    constexpr size_t M = 5;
    std::vector<int> more_fibs;
    more_fibs.reserve(M);
    std::copy_n(AIO::CoroutineIterator(fib), M, std::back_inserter(more_fibs));
    std::cout << "More fibs: ";
    for (const int e : more_fibs) {
        std::cout << e << ' ';
    }
    std::cout << "..." << std::endl;


    std::cout << "Until MAX=" << MAX << ": ";
    for (const int e : AIO::CoroutineGenerator(fib)) {
        std::cout << e << ' ';
    }
    std::cout << std::endl;
}

int main() {
    sample_contexts();
    sample_coroutines();
}
