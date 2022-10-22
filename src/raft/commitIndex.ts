import { LeaderState } from './state';

// Calculate the next commit index from updated state.
export function commitIndexFromState<T>(state: LeaderState<T>) {
    const {
        log,
        followerInfo,
        currentTerm,
        commitIndex: currentCommitIndex,
    } = state;
    const logSize = log.getEntries().length;
    const otherNodesMatchIndex = Object.values(followerInfo).map(
        ({ matchIndex }) => matchIndex,
    );
    const matchIndex = [logSize - 1, ...otherNodesMatchIndex];
    matchIndex.sort();
    const potentialCommitIndex =
        matchIndex[Math.floor((matchIndex.length - 1) / 2)];
    
    if (potentialCommitIndex === -1) {
        return currentCommitIndex;
    }

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    if (log.getEntries()[potentialCommitIndex!]!.term === currentTerm) {
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        return Math.max(currentCommitIndex, potentialCommitIndex!);
    }

    return currentCommitIndex;
}
