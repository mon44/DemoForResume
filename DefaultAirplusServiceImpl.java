package com.ctrip.corp.settlement.airplus.business.common;

import com.ctrip.corp.foundation.common.plus.executor.ExecutorServiceMgr;
import com.ctrip.corp.foundation.log.service.LoggerService;
import com.ctrip.corp.foundation.log.service.LoggerServiceFactory;
import com.ctrip.corp.settlement.airplus.business.common.api.AirplusService;
import com.ctrip.corp.settlement.airplus.business.common.api.DataConsumer;
import com.ctrip.corp.settlement.airplus.business.common.api.DataPredicate;
import com.ctrip.corp.settlement.airplus.business.common.api.DataSupplier;
import com.ctrip.corp.settlement.airplus.business.common.api.FilteredDataProcessor;
import com.ctrip.corp.settlement.airplus.business.common.api.SingleDataConsumer;
import com.ctrip.corp.settlement.airplus.business.common.entity.SettlementContext;
import com.ctrip.corp.settlement.airplus.business.common.entity.SettlementParameter;
import com.ctrip.corp.settlement.airplus.business.common.exception.ProcessException;
import lombok.Builder;
import org.apache.commons.collections.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author zwyu
 * @date 2020-07-15
 */
@Builder
public class DefaultAirplusServiceImpl<T, R> implements AirplusService {
    private static LoggerService log = LoggerServiceFactory.getLoggerService(DefaultAirplusServiceImpl.class);
    private static final String CONSUMER_ERROR = "consumer error.";
    private DataSupplier<T> dataSupplier;
    private Function<T, R> dataConvert;
    private List<DataPredicate<R>> predicates;
    private DataConsumer<R> dataConsumer;
    private SingleDataConsumer<R> singleDataConsumer;
    private int bufferSize;
    // 前处理器
    private Consumer<SettlementContext> preProcessor;
    // 后处理器
    private Consumer<SettlementContext> finalProcessor;
    private FluxSink<R> filteredFluxSink;
    private FilteredDataProcessor<R> filteredDataProcessor;
    @Builder.Default
    private ThreadPoolExecutor filteredThreadPoolExecutor =
            ExecutorServiceMgr.getExecutorService("filteredThreadPoolExecutor");

    @Override
    public boolean handle(SettlementParameter par) {
        SettlementContext context = buildSettlementContext(par);
        if (preProcessor != null) {
            preProcessor.accept(context);
        }
        process(context);
        if (finalProcessor != null) {
            finalProcessor.accept(context);
        }
        return true;
    }

    private void process(SettlementContext settlementContext) {
        if (filteredDataProcessor != null) {
            filteredProcess(settlementContext);
        }
        Flux<R> flux = buildFlux(settlementContext).filter(d -> predicate(d, settlementContext));
        if (bufferSize > 0) {
            flux.buffer(bufferSize).subscribe(d -> subscribe(d, settlementContext));
        } else {
            flux.subscribe(d -> subscribe(d, settlementContext));
        }
    }

    private Flux<R> buildFlux(SettlementContext context) {
        return Flux.create(sink -> {
            while (true) {
                Supplier<List<T>> supplier = new RetrySupplier<>(() -> dataSupplier.get(context));
                List<T> sourceData = supplier.get();
                if (CollectionUtils.isEmpty(sourceData)) {
                    sink.complete();
                    if (filteredFluxSink != null) {
                        filteredFluxSink.complete();
                    }
                    return;
                }
                if (dataConvert != null) {
                    Flux.fromIterable(sourceData).map(p -> tryConvert(dataConvert, p, context))
                            .filter(p -> p != null).subscribe(p -> sink.next(p));
                }
            }
        });
    }

    private R tryConvert(Function<T, R> dataConvert, T data, SettlementContext context) {
        try {
            return dataConvert.apply(data);
        } catch (Exception e) {
            context.getHasException().set(false);
            log.warn("convert error.", "error:{} value:{}", e, "");
            return null;
        }
    }

    private boolean predicate(R data, SettlementContext context) {
        return Optional.ofNullable(predicates)
                .map(p -> p.stream().allMatch(predicate -> tryPredicate(predicate, data, context))).orElse(true);
    }

    private boolean tryPredicate(DataPredicate predicate, R data, SettlementContext context) {
        try {
            boolean result = predicate.test(data, context);
            if (!result && filteredFluxSink != null) {
                filteredFluxSink.next(data);
            }
            return result;
        } catch (Exception e) {
            context.getHasException().set(false);
            log.warn("predicate error.", "error:{} data:{}", e, "");
            return false;
        }
    }

    private void subscribe(List<R> datas, SettlementContext context) {
        try {
            dataConsumer.apply(datas, context);
        } catch (Exception e) {
            context.getHasException().set(false);
            log.error(CONSUMER_ERROR, e);
        }
    }

    private void subscribe(R data, SettlementContext context) {
        try {
            singleDataConsumer.apply(data, context);
        } catch (Exception e) {
            context.getHasException().set(false);
            log.error(CONSUMER_ERROR, e);
        }
    }

    private SettlementContext buildSettlementContext(SettlementParameter par) {
        SettlementContext context = new SettlementContext();
        context.setSettlementParameter(par);
        context.setHasException(new AtomicBoolean(true));
        return context;
    }

    private void filteredProcess(SettlementContext context) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        filteredThreadPoolExecutor.submit(() -> Flux.<R>create(p -> {
            filteredFluxSink = p;
            countDownLatch.countDown();
        }).buffer(100).subscribe(p -> filteredDataProcessor.process(p, context)));
        try {
            countDownLatch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("countDownLatch.await error.", e);
            throw new ProcessException("countDownLatch.await error.", e);
        }
    }
}
