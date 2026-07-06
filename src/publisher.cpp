#include "DdsPerfCommon.hpp"

#include <csignal>
#include <iomanip>
#include <limits>
#include <vector>

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

        const auto options = dperf::parse_options(argc, argv, true);
        dperf::DdsEntities dds(options.domain);
        auto* request_topic = dds.topic(dperf::kRequestTopic);
        auto* reply_topic = dds.topic(dperf::kReplyTopic);
        auto* writer = dds.writer(request_topic, options.reliability);
        auto* reader = dds.reader(reply_topic, options.reliability);

        PerfMessage outgoing;
        outgoing.payload(std::vector<uint8_t>(options.size, 0x5a));

        PerfMessage incoming;
        SampleInfo info;

        uint64_t sent = 0;
        uint64_t received = 0;
        uint64_t last_print_sent = 0;
        uint64_t last_print_received = 0;
        uint64_t last_print_ns = dperf::now_ns();
        uint64_t latency_sum_ns = 0;
        uint64_t latency_min_ns = std::numeric_limits<uint64_t>::max();
        uint64_t latency_max_ns = 0;
        uint64_t bytes_received = 0;

        std::cout << "publisher started: size=" << options.size
                  << " qos=" << (options.reliability == dperf::Reliability::Reliable ? "reliable" : "best_effort")
                  << " domain=" << options.domain << '\n';

        if (options.warmup_ms > 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(options.warmup_ms));
        }

        while (g_running && (options.samples == 0 || sent < options.samples))
        {
            outgoing.seq(sent + 1);
            outgoing.send_time_ns(dperf::now_ns());
            if (writer->write(&outgoing))
            {
                ++sent;
            }

            while (reader->take_next_sample(&incoming, &info) == ReturnCode_t::RETCODE_OK)
            {
                if (!info.valid_data)
                {
                    continue;
                }
                const uint64_t now = dperf::now_ns();
                const uint64_t rtt_ns = now > incoming.send_time_ns() ? now - incoming.send_time_ns() : 0;
                const uint64_t one_way_ns = rtt_ns / 2;
                ++received;
                bytes_received += incoming.payload().size();
                latency_sum_ns += one_way_ns;
                latency_min_ns = std::min(latency_min_ns, one_way_ns);
                latency_max_ns = std::max(latency_max_ns, one_way_ns);
            }

            const uint64_t now = dperf::now_ns();
            if (now - last_print_ns >= 1000000000ULL)
            {
                const double elapsed_s = static_cast<double>(now - last_print_ns) / 1e9;
                const uint64_t interval_received = received - last_print_received;
                const uint64_t interval_sent = sent - last_print_sent;
                const double mbps = (static_cast<double>(interval_received) * options.size * 8.0) / elapsed_s / 1000000.0;
                const double loss = interval_sent == 0
                        ? 0.0
                        : 100.0 * static_cast<double>(interval_sent - std::min(interval_sent, interval_received))
                                  / static_cast<double>(interval_sent);
                const double avg_latency_us = received == 0
                        ? 0.0
                        : (static_cast<double>(latency_sum_ns) / static_cast<double>(received)) / 1000.0;
                const double min_latency_us = latency_min_ns == std::numeric_limits<uint64_t>::max()
                        ? 0.0
                        : static_cast<double>(latency_min_ns) / 1000.0;
                const double max_latency_us = static_cast<double>(latency_max_ns) / 1000.0;

                std::cout << std::fixed << std::setprecision(3)
                          << "sent=" << sent
                          << " received=" << received
                          << " throughput=" << mbps << " Mbps"
                          << " latency_avg=" << avg_latency_us << " us"
                          << " latency_min=" << min_latency_us << " us"
                          << " latency_max=" << max_latency_us << " us"
                          << " loss=" << loss << " %"
                          << '\n';

                last_print_sent = sent;
                last_print_received = received;
                last_print_ns = now;
            }

            if (options.interval_us > 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(options.interval_us));
            }
        }

        const auto drain_until = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        while (std::chrono::steady_clock::now() < drain_until)
        {
            if (reader->take_next_sample(&incoming, &info) == ReturnCode_t::RETCODE_OK && info.valid_data)
            {
                const uint64_t now = dperf::now_ns();
                const uint64_t rtt_ns = now > incoming.send_time_ns() ? now - incoming.send_time_ns() : 0;
                ++received;
                bytes_received += incoming.payload().size();
                latency_sum_ns += rtt_ns / 2;
            }
            else
            {
                dperf::tiny_sleep();
            }
        }

        const double total_loss = sent == 0
                ? 0.0
                : 100.0 * static_cast<double>(sent - std::min(sent, received)) / static_cast<double>(sent);
        const double avg_latency_us = received == 0
                ? 0.0
                : (static_cast<double>(latency_sum_ns) / static_cast<double>(received)) / 1000.0;

        std::cout << "summary: sent=" << sent
                  << " received=" << received
                  << " bytes=" << bytes_received
                  << " avg_latency=" << std::fixed << std::setprecision(3) << avg_latency_us << " us"
                  << " loss=" << total_loss << " %"
                  << '\n';
        return 0;
    }
    catch (const std::exception& e)
    {
        std::cerr << "publisher error: " << e.what() << '\n';
        dperf::print_common_usage(argv[0], true);
        return 1;
    }
}
