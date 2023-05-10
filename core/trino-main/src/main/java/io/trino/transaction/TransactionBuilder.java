/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.transaction;

import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.session.SessionConfigurationContext;
import io.trino.spi.transaction.TransactionId;
import io.trino.spi.transaction.TransactionInfo;
import io.trino.spi.transaction.TransactionManager;
import io.trino.spi.transaction.TransactionalSession;

import javax.inject.Inject;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class TransactionBuilder {

    private IsolationLevel isolationLevel = TransactionManager.DEFAULT_ISOLATION;
    private boolean readOnly = TransactionManager.DEFAULT_READ_ONLY;
    private boolean singleStatement;

    @Inject
    public TransactionBuilder(TransactionExecutor transactionExecutor) {
        this.transactionExecutor = transactionExecutor;
    }

    public <T> T executeTransaction(SessionConfigurationContext sessionContext, IsolationLevel isolationLevel,
                                     boolean readOnly, Function<TransactionalSession, T> callback) {
        return transactionExecutor.executeTransaction(sessionContext, isolationLevel, readOnly, callback);
    }

    public void executeTransaction(SessionConfigurationContext sessionContext, IsolationLevel isolationLevel,
                                    boolean readOnly, Consumer<TransactionalSession> callback) {
        transactionExecutor.executeTransaction(sessionContext, isolationLevel, readOnly, (TransactionalSession session) -> {
            callback.accept(session);
            return null;
        });
    }
}
public class TransactionExecutor {
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    @Inject
    public TransactionExecutor(TransactionManager transactionManager, AccessControl accessControl) {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public <T> T executeTransaction(SessionConfigurationContext sessionContext, IsolationLevel isolationLevel,
                                     boolean readOnly, Function<TransactionalSession, T> callback) {
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(callback, "callback is null");

        TransactionId transactionId = transactionManager.beginTransaction(isolationLevel, readOnly, false);

        boolean success = false;
        try {
            Optional<TransactionInfo> transactionInfo = transactionManager.getTransactionInfo(transactionId);
            TransactionalSession transactionSession = new TransactionalSession(sessionContext, transactionId, transactionManager, accessControl, transactionInfo.get(), false);
            T result = callback.apply(transactionSession);
            success = true;
            return result;
        } finally {
            if (success) {
                getFutureValue(transactionManager.asyncCommit(transactionId));
            } else {
                transactionManager.asyncAbort(transactionId);
            }
        }
    }
}
