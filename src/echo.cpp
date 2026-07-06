#include "DdsPerfCommon.hpp"

#include <csignal>

namespace
{
std::atomic_bool g_running{true};

void handle_signal(int)
{
    g_running = false;
}
}

int main(int argc, char** argv)
{
    using eprosima::fastdds::dds::SampleInfo;
    using eprosima::fastrtps::types::ReturnCode_t;

    try
    {
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);

        const auto options = dperf::parse_options(argc, argv, false);
        dperf::DdsEntities dds(options.domain);
        auto* request_topic = dds.topic(dperf::kRequestTopic);
        auto* reply_topic = dds.topic(dperf::kReplyTopic);
        auto* reader = dds.reader(request_topic, options.reliability);
        auto* writer = dds.writer(reply_topic, options.reliability);

        PerfMessage message;
        SampleInfo info;
        uint64_t echoed = 0;
        uint64_t last_echoed = 0;
        uint64_t last_print_ns = dperf::now_ns();

        std::cout << "echo started: size=" << options.size
                  << " qos=" << (options.reliability == dperf::Reliability::Reliable ? "reliable" : "best_effort")
                  << " domain=" << options.domain << '\n';

        while (g_running)
        {
            if (reader->take_next_sample(&message, &info) == ReturnCode_t::RETCODE_OK)
            {
                if (info.valid_data)
                {
                    writer->write(&message);
                    ++echoed;
                }
            }
            else
            {
                dperf::tiny_sleep();
            }

            const uint64_t now = dperf::now_ns();
            if (now - last_print_ns >= 1000000000ULL)
            {
                const double elapsed_s = static_cast<double>(now - last_print_ns) / 1e9;
                const double rate = static_cast<double>(echoed - last_echoed) / elapsed_s;
                std::cout << "echoed=" << echoed << " rate=" << rate << " msg/s" << '\n';
                last_echoed = echoed;
                last_print_ns = now;
            }
        }

        std::cout << "summary: echoed=" << echoed << '\n';
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "echo error: " << e.what() << '\n';
        dperf::print_common_usage(argv[0], false);
        return 1;
    }
}
